pub mod error;

use std::any::{type_name, Any, TypeId};
use std::collections::{HashMap, HashSet};
use std::fmt::Display;
use std::hash::Hash;
use std::sync::{Arc, Weak};

use futures_util::future;
use futures_util::stream::{self, StreamExt as _, TryStreamExt as _};
use parking_lot::Mutex;
use tokio::sync::broadcast;

pub use self::error::Error;

#[derive(Clone)]
pub struct Dataloader<T> {
    state: Arc<Mutex<HashMap<(TypeId, TypeId), Box<dyn Any + Send>>>>,
    loader: T,
}

impl<T> Dataloader<T> {
    pub fn new(loader: T) -> Self {
        Self {
            state: Default::default(),
            loader,
        }
    }

    #[tracing::instrument(skip_all)]
    pub async fn load_one<K, V>(&self, key: K) -> Result<Option<V>, Error>
    where
        K: Clone + Eq + Hash + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
        T: Loader<K, V> + Clone + Send + Sync + 'static,
        T::Error: Clone + Display + Send + Sync + 'static,
    {
        self.load_many([key])
            .await
            .map(|vec| vec.into_iter().next())
    }

    #[tracing::instrument(skip_all)]
    pub async fn load_many<I, K, V>(&self, keys: I) -> Result<Vec<V>, Error>
    where
        I: IntoIterator<Item = K>,
        K: Clone + Eq + Hash + Send + Sync + 'static,
        V: Clone + Send + Sync + 'static,
        T: Loader<K, V> + Clone + Send + Sync + 'static,
        T::Error: Clone + Display + Send + Sync + 'static,
    {
        let inner = {
            let mut state = self.state.lock();

            state
                .entry((TypeId::of::<K>(), TypeId::of::<V>()))
                .or_insert_with(|| Box::new(DataloaderInner::<K, V, T::Error>::default()))
                .downcast_ref::<DataloaderInner<K, V, T::Error>>()
                .expect(&*format!(
                    "`Dataloader<{}>` -> `DataloaderInner<{}, {}, {}>` downcast failed",
                    type_name::<T>(),
                    type_name::<K>(),
                    type_name::<V>(),
                    type_name::<T::Error>(),
                ))
                .clone()
        };

        inner.load_many(self.loader.clone(), keys).await
    }

    pub fn loader(&self) -> &T {
        &self.loader
    }
}

struct DataloaderInner<K, V, E> {
    state: Arc<Mutex<HashMap<K, DataloaderInnerState<K, V, E>>>>,
}

impl<K, V, E> DataloaderInner<K, V, E> {
    #[tracing::instrument(skip_all)]
    async fn load_many<I, T>(&self, loader: T, keys: I) -> Result<Vec<V>, Error>
    where
        I: IntoIterator<Item = K>,
        K: Clone + Eq + Hash + Send + Sync + 'static,
        V: Clone + Send + 'static,
        T: Loader<K, V, Error = E> + Clone + Send + Sync + 'static,
        T::Error: Clone + Display + Send + 'static,
    {
        let keys: HashSet<_> = keys.into_iter().collect();

        let rx = {
            let mut state = self.state.lock();

            let mut pending = HashMap::with_capacity(keys.len());
            let mut ready = HashMap::with_capacity(keys.len());
            let mut unseen = Vec::with_capacity(keys.len());

            for key in keys {
                match state.get(&key) {
                    Some(DataloaderInnerState::Pending(tx)) => match Weak::upgrade(tx) {
                        Some(tx) => {
                            pending.insert(key, tx.subscribe());
                        }
                        None => {
                            unseen.push(key);
                        }
                    },
                    Some(DataloaderInnerState::Ready(result)) => {
                        ready.insert(key, result);
                    }
                    None => {
                        unseen.push(key);
                    }
                }
            }

            if pending.len() == 0 && unseen.len() == 0 {
                return ready
                    .into_iter()
                    .filter_map(|(key, result)| match result {
                        Ok(map) => map.get(&key).cloned().map(Ok),
                        Err(e) => Some(Err(Error::loader(e))),
                    })
                    .collect();
            }

            let pending = stream::iter(
                ready
                    .into_iter()
                    .filter_map(|(key, result)| match result {
                        Ok(map) => map.get(&key).cloned().map(Ok),
                        Err(e) => Some(Err(Error::loader(e))),
                    })
                    .collect::<Vec<_>>(),
            )
            .chain(
                stream::iter(pending)
                    .then(|(key, mut rx)| async move {
                        Ok(rx
                            .recv()
                            .await
                            .map_err(Error::recv)?
                            .map(|mut map| map.remove(&key))
                            .map_err(Error::loader)?)
                    })
                    .try_filter_map(|value| future::ready(Ok(value))),
            );

            if unseen.len() == 0 {
                pending.boxed()
            } else {
                let (tx, mut rx) = broadcast::channel(1);

                let tx = Arc::new(tx);
                for key in unseen.clone() {
                    state.insert(key, DataloaderInnerState::Pending(Arc::downgrade(&tx)));
                }

                tokio::spawn({
                    let state = Arc::clone(&self.state);

                    async move {
                        let result = loader.load(&*unseen).await;

                        let mut state = state.lock();
                        for key in unseen {
                            state.insert(key, DataloaderInnerState::Ready(result.clone()));
                        }

                        tx.send(result).ok()
                    }
                });

                let unseen = stream::once(Box::pin(async move {
                    let result = rx.recv().await;

                    match result {
                        Ok(Ok(map)) => stream::iter(map.into_values().map(Ok).collect::<Vec<_>>()),
                        Ok(Err(e)) => stream::iter(vec![Err(Error::loader(e))]),
                        Err(e) => stream::iter(vec![Err(Error::recv(e))]),
                    }
                }))
                .flatten();

                pending.chain(unseen).boxed()
            }
        };

        rx.try_collect().await
    }
}

impl<K, V, E> Clone for DataloaderInner<K, V, E> {
    fn clone(&self) -> Self {
        Self {
            state: Arc::clone(&self.state),
        }
    }
}

impl<K, V, E> Default for DataloaderInner<K, V, E> {
    fn default() -> Self {
        Self {
            state: Default::default(),
        }
    }
}

enum DataloaderInnerState<K, V, E> {
    Pending(Weak<broadcast::Sender<LoaderResult<K, V, E>>>),
    Ready(LoaderResult<K, V, E>),
}

type LoaderResult<K, V, E> = Result<HashMap<K, V>, E>;

#[async_trait::async_trait]
pub trait Loader<K, V> {
    type Error;

    async fn load(&self, keys: &[K]) -> LoaderResult<K, V, Self::Error>;
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::convert::Infallible;
    use std::time::{Duration, Instant};

    use futures_util::stream::{self, StreamExt as _};

    #[derive(Clone)]
    struct Loader<const N: i32>;

    #[async_trait::async_trait]
    impl<const N: i32> super::Loader<i32, i32> for Loader<N> {
        type Error = Infallible;

        async fn load(&self, keys: &[i32]) -> Result<HashMap<i32, i32>, Self::Error> {
            Ok(stream::iter(keys.to_vec())
                .map(|i| async move {
                    tokio::time::sleep(Duration::from_secs(1)).await;

                    (i, i * N)
                })
                .buffer_unordered(1024)
                .collect()
                .await)
        }
    }

    #[tokio::test]
    async fn test_caching() {
        let dataloader = super::Dataloader::new(Loader::<2> {});
        dataloader.load_many(1..=100).await.unwrap();

        let start = Instant::now();

        dataloader.load_many(1..=100).await.unwrap();

        let duration = Instant::now().duration_since(start);

        assert!(duration.as_secs() == 0);
    }

    #[tokio::test]
    async fn test_concurrency_one() {
        let start = Instant::now();

        let dataloader = super::Dataloader::new(Loader::<2> {});
        dataloader.load_many(1..=100).await.unwrap();

        let duration = Instant::now().duration_since(start);

        assert!(duration.as_secs() == 1);
    }

    #[tokio::test]
    async fn test_concurrency_many() {
        let start = Instant::now();

        let dataloader2 = super::Dataloader::new(Loader::<2> {});
        let dataloader10 = super::Dataloader::new(Loader::<10> {});

        futures_util::try_join!(
            dataloader2.load_many(1..=100),
            dataloader10.load_many(1..=100),
        )
        .unwrap();

        let duration = Instant::now().duration_since(start);

        assert!(duration.as_secs() == 1);
    }

    #[tokio::test]
    async fn test_correctness() {
        let dataloader = super::Dataloader::new(Loader::<2> {});

        let mut values = dataloader.load_many(1..=3).await.unwrap();
        values.sort();
        assert_eq!(values, [2, 4, 6]);

        let mut values = dataloader.load_many(1..=3).await.unwrap();
        values.sort();
        assert_eq!(values, [2, 4, 6]);

        let mut values = dataloader.load_many(1..=5).await.unwrap();
        values.sort();
        assert_eq!(values, [2, 4, 6, 8, 10]);
    }
}

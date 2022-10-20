use std::fmt::{self, Debug, Display};

use tokio::sync::broadcast;

pub struct Error {
    inner: ErrorInner,
}

impl Error {
    pub(crate) fn loader<E>(source: E) -> Self
    where
        E: Display,
    {
        Self {
            inner: ErrorInner {
                kind: ErrorKind::Loader,
                source: Some(source.to_string().into()),
            },
        }
    }

    pub(crate) fn recv(source: broadcast::error::RecvError) -> Self {
        Self {
            inner: ErrorInner {
                kind: ErrorKind::Recv,
                source: Some(Box::new(source)),
            },
        }
    }

    pub fn kind(&self) -> ErrorKind {
        self.inner.kind()
    }
}

impl Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error")
            .field("kind", &self.inner.kind)
            .field("source", &self.inner.source)
            .finish()
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.inner.source()
    }
}

#[derive(Debug)]
struct ErrorInner {
    kind: ErrorKind,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl ErrorInner {
    fn kind(&self) -> ErrorKind {
        self.kind
    }
}

impl Display for ErrorInner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match self.kind {
            ErrorKind::Loader => "loader error",
            ErrorKind::Recv => "recv error",
        };

        if let Some(ref source) = self.source {
            write!(f, "{}: {}", kind, source)
        } else {
            f.write_str(kind)
        }
    }
}

impl std::error::Error for ErrorInner {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.source {
            Some(ref source) => Some(&**source),
            None => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Loader,
    Recv,
}

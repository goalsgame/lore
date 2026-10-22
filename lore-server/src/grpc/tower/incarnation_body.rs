// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use futures::future::BoxFuture;
use futures::ready;
use http::Request;
use http_body::Body;
use http_body::Frame;
use http_body::SizeHint;
use pin_project::pin_project;
use tonic::Status;
use tower::Layer;
use tower::Service;

use crate::authnz::repository_authorizer::IncarnationGuard;
use crate::authnz::repository_authorizer::PartitionGrants;
use crate::authnz::repository_authorizer::RepositoryAuthorizer;
use crate::grpc::get_verified_token;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Re-checks a name-scoped request's incarnation as each part of its body
/// arrives, so a unary body delivered after the repository was recreated is
/// refused before the handler sees it.
///
/// Sits inside the [`PartitionAccessLayer`](super::partition_access::PartitionAccessLayer),
/// which records the incarnation in the request's [`PartitionGrants`].
/// Requests without one are passed through untouched and pay nothing.
#[derive(Clone)]
pub struct IncarnationBodyLayer {
    authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl IncarnationBodyLayer {
    pub fn new(authorizer: Arc<dyn RepositoryAuthorizer>) -> Self {
        Self { authorizer }
    }
}

impl<S> Layer<S> for IncarnationBodyLayer {
    type Service = IncarnationBodyService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        IncarnationBodyService {
            inner,
            authorizer: self.authorizer.clone(),
        }
    }
}

#[derive(Clone)]
pub struct IncarnationBodyService<S> {
    inner: S,
    authorizer: Arc<dyn RepositoryAuthorizer>,
}

impl<S, B> Service<Request<B>> for IncarnationBodyService<S>
where
    S: Service<Request<IncarnationBody<B>>>,
    B: Body,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let (parts, body) = request.into_parts();
        let guard = parts
            .extensions
            .get::<PartitionGrants>()
            .and_then(|grants| {
                IncarnationGuard::new(
                    self.authorizer.clone(),
                    get_verified_token(&parts.extensions).as_ref(),
                    grants.repository_id,
                    grants.incarnation,
                )
            });
        self.inner.call(Request::from_parts(
            parts,
            IncarnationBody {
                inner: body,
                guard,
                pending: None,
                failed: false,
            },
        ))
    }
}

type Pending<D> = (
    BoxFuture<'static, (IncarnationGuard, Result<(), Status>)>,
    Option<Frame<D>>,
);

#[pin_project]
pub struct IncarnationBody<B: Body> {
    #[pin]
    inner: B,
    guard: Option<IncarnationGuard>,
    pending: Option<Pending<B::Data>>,
    failed: bool,
}

impl<B> Body for IncarnationBody<B>
where
    B: Body,
    B::Error: Into<BoxError>,
{
    type Data = B::Data;
    type Error = BoxError;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        loop {
            if let Some((check, _)) = this.pending.as_mut() {
                let (guard, verdict) = ready!(check.as_mut().poll(cx));
                let (_, frame) = this.pending.take().expect("a check is pending");
                *this.guard = Some(guard);
                return Poll::Ready(match verdict {
                    Ok(()) => frame.map(Ok),
                    Err(status) => {
                        *this.failed = true;
                        Some(Err(Box::new(status)))
                    }
                });
            }
            if *this.failed {
                return Poll::Ready(None);
            }
            match ready!(this.inner.as_mut().poll_frame(cx)) {
                None => return Poll::Ready(None),
                Some(Err(error)) => return Poll::Ready(Some(Err(error.into()))),
                Some(Ok(frame)) => {
                    if frame.is_data()
                        && let Some(mut guard) = this.guard.take()
                    {
                        let check = Box::pin(async move {
                            let verdict = guard.ensure_unchanged().await;
                            (guard, verdict)
                        });
                        *this.pending = Some((check, Some(frame)));
                        continue;
                    }
                    return Poll::Ready(Some(Ok(frame)));
                }
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        self.pending.is_none() && self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl<S: tonic::server::NamedService> tonic::server::NamedService for IncarnationBodyService<S> {
    const NAME: &'static str = S::NAME;
}

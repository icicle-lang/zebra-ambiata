module Zebra.X.Either (
    hoistWith
  ) where

import           X.Control.Monad.Trans.Either

hoistWith :: Monad m => (a -> x) -> Either a b -> EitherT x m b
hoistWith f = firstEitherT f . hoistEither

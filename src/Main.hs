{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE PartialTypeSignatures #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Frames hiding ((:&))
import Frames.CSV (tableTypesOverride, RowGen(..), rowGen, ReadRec)
import Control.Monad.IO.Class (MonadIO)
import Data.Vinyl (Rec, RecApplicative(rpure), rmap, rapply)
import Data.Vinyl.Functor (Lift(..), Identity(..))
import Control.Lens (view, (&), (?~))
import Pipes (Pipe,Producer, (>->), runEffect)
import Data.Monoid ((<>),First(..))
import Data.Maybe (fromMaybe)
import Control.Lens.Getter(Getting )
import qualified Pipes.Prelude as P
import qualified Data.Text.Lazy as LT
import qualified Data.Map as M
import qualified Data.Text.Format as T

tableTypesOverride rowGen {rowTypeName ="Normalized"} "normalized.csv" [("composite_key", ''Text)]
tableTypesOverride rowGen {rowTypeName ="Denormalized"} "denormalized.csv" []

-- An en passant Default class
class Default a where
  def :: a

instance Default (s :-> Int) where def = Col 0
instance Default (s :-> Text) where def = Col mempty
instance Default (s :-> Double) where def = Col 0.0
instance Default (s :-> Bool) where def = Col False

-- We can write instances for /all/ 'Rec' values.
instance (Applicative f, LAll Default ts, RecApplicative ts)
  => Default (Rec f ts) where
  def = reifyDict [pr|Default|] (pure def)

mkCompositeKey' :: ( RecApplicative rs2
                  , KeyA ∈ rs
                  , KeyB ∈ rs
                  , KeyC ∈ rs
                  , KeyD ∈ rs
                  , CompositeKey ∈ rs2
                  ) => Record rs -> Record rs2
mkCompositeKey' denormRow = fromMaybe (error "failed to make composite key") . recMaybe $ newRow
  where newRow = rpure Nothing & compositeKey' ?~ Col compositeKeyTxt
        compositeKeyTxt = view' keyA <> view' keyB <> view' keyC <> view' keyD
        view' l = LT.toStrict $ T.format "{}" (T.Only (view l denormRow))

mkCompositeKey :: Text -> Text -> Text -> Text -> Text
mkCompositeKey a b c d = a <> b <> c <> d

defaultingProducer :: ( ReadRec rs
                      , RecApplicative rs
                      , MonadIO m
                      , LAll Default rs
                      ) => FilePath -> String -> Producer (Rec Identity rs) m ()
defaultingProducer fp label = readTableMaybe fp >-> P.map (holeFiller label)

recMaybe'' :: forall rs. Rec Maybe rs -> Maybe (Record rs)
recMaybe'' = recMaybe

holeFiller :: forall ty.
              ( LAll Default ty
              , RecApplicative ty
              ) => String -> Rec Maybe ty -> Rec Identity ty
holeFiller label rec1 = let fromJust = fromMaybe (error $ label ++ " failure")
                            firstRec1 :: Rec First ty
                            firstRec1 = rmap First rec1
                            concattedStuff :: Rec First ty
                            concattedStuff = rapply (rmap (Lift . flip (<>)) def) firstRec1
                            firsts :: Rec Maybe ty
                            firsts = rmap getFirst concattedStuff
                            recMaybed :: Maybe (Rec Identity ty)
                            recMaybed = recMaybe firsts
                        in fromJust recMaybed

-- fills in empty values with holeFiller and Defaults typeclass instances
normalized :: Producer Normalized IO ()
-- normalized = readTableMaybe "normalized.csv" >-> P.map (holeFiller "normalized")
normalized = defaultingProducer "normalized.csv" "normalized"

type DenormalizedWithCompositeKey = Record (CompositeKey ': RecordColumns Denormalized)
-- fills in empty values with holeFiller and Defaults typeclass instances
denormalized :: Producer DenormalizedWithCompositeKey IO ()
denormalized = defaultingProducer "denormalized.csv" "denormalized"
               >-> P.map appendCompositeKey

appendCompositeKey :: forall (rs :: [*]).
                      ( KeyA ∈ rs
                      , KeyB ∈ rs
                      , KeyC ∈ rs
                      , KeyD ∈ rs
                      ) => Record rs -> Record (CompositeKey ': rs)
appendCompositeKey inRecord = frameConsA compositeKeyTxt inRecord
  where compositeKeyTxt = mkCompositeKey
                            (viewToTxt keyA inRecord)
                            (viewToTxt keyB inRecord)
                            (viewToTxt keyC inRecord)
                            (viewToTxt keyD inRecord)
        viewToTxt l r = intToTxt (view l r)
        intToTxt i = LT.toStrict $ T.format "{}" (T.Only i)

addCompositeKeyToMap :: ( Num a
                        , CompositeKey ∈ rs
                        ) => M.Map Text a -> Record rs -> M.Map Text a
addCompositeKeyToMap m r = M.insert (view compositeKey r) 0 m

findMissingRows :: forall rs1 rs2 m. ( Monad m
                                 , CompositeKey ∈ rs1
                                 , CompositeKey ∈ rs2
                                 ) => Producer (Record rs1) m () -> m (Pipe (Record rs2) (Record rs2) m ())
findMissingRows checkProducer = do
  -- read a map of the checkProducer into memory
  compositeKeyMap <- P.fold (\m r -> addCompositeKeyToMap m r) M.empty id checkProducer
  pure $ P.filter (\r -> M.notMember (view compositeKey r) (compositeKeyMap :: M.Map Text Integer))

-- -- TODO allow providing a lens to find missing rows on
findMissingRowsOn :: forall (checkRec :: [*]) (outRec :: [*])  (monad :: * -> *) (key :: *).
                     ( Monad monad
                     , Ord key
                     , Show key
                     ) =>
                     Getting key (Rec Identity checkRec) key -- lens
                  -> Getting key (Rec Identity outRec) key -- lens
                  -> Producer (Record checkRec) monad ()     -- checkProducer
                  -> monad (Pipe (Record outRec) (Record outRec) monad ())
findMissingRowsOn lens1 lens2 checkProducer = do
  keyMap <- P.fold (\m r -> M.insert (view lens1 (r :: Record checkRec)) 0 m) M.empty id checkProducer
  pure $ P.filter (\(r :: Record rec2) -> M.notMember (view lens2 (r :: Record outRec))  keyMap)


-- attempt to remove the need for passing the same lens twice
-- currently fails because the same lens specializes to two different types and I don't know how to say "lens should work for both CheckRec and outRec"
-- findMissingRowsOn' :: forall (checkRec :: [*]) (outRec :: [*]) (bothRec :: [*]) (monad :: * -> *) (key :: *).
--                      ( Monad monad
--                      , Ord key
--                      , Show key
--                      ) =>
--                      Getting key _ key -- lens
--                   -> Producer (Record checkRec) monad ()     -- checkProducer
--                   -> monad (Pipe (Record outRec) (Record outRec) monad ())
-- findMissingRowsOn' lens checkProducer = do
--   keyMap <- P.fold (\m r -> M.insert (view lens (r :: Record checkRec)) 0 m) M.empty id checkProducer
--   pure $ P.filter (\(r :: Record rec2) -> M.notMember (view lens (r :: Record outRec))  keyMap)

-- src/Main.hs:153:56: error: …
--     • Could not deduce (Control.Monad.Reader.Class.MonadReader
--                           (Rec Identity checkRec) ((->) (Record outRec)))
--         arising from a use of ‘view’
--       from the context: (Ord key, Monad monad)
--         bound by the inferred type of
--                  findMissingRowsOn' :: (Ord key, Monad monad) =>
--                                        Getting key (Rec Identity checkRec) key
--                                        -> Producer (Record checkRec) monad ()
--                                        -> monad (Pipe (Record outRec) (Record outRec) monad ())
--         at /home/cody/source/frames-differences/src/Main.hs:(151,1)-(153,95)
--     • In the first argument of ‘M.notMember’, namely
--         ‘(view lens (r :: Record outRec))’
--       In the expression:
--         M.notMember (view lens (r :: Record outRec)) keyMap
--       In the first argument of ‘P.filter’, namely
--         ‘(\ (r :: Record rec2)
--             -> M.notMember (view lens (r :: Record outRec)) keyMap)’
-- Compilation failed.



printMissingRows :: IO ()
printMissingRows = do
  putStrLn "rows normalized contains denormalized does not"
  findMissing <- findMissingRowsOn compositeKey compositeKey denormalized
  runEffect $ normalized >-> findMissing >-> P.print

  -- try out new version that only requires specifying key once
  -- putStrLn "rows normalized contains denormalized does not"
  -- findMissing <- findMissingRowsOn' compositeKey denormalized
  -- runEffect $ normalized >-> findMissing >-> P.print


main :: IO ()
main = undefined

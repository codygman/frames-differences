{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeOperators #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Main where

import Frames hiding ((:&))
import Frames.CSV (tableTypesOverride, RowGen(..), rowGen, readTableOpt, ReadRec)
import Frames.InCore (RecVec)
import Control.Monad.IO.Class (MonadIO)
import qualified Data.Vinyl as V
import Data.Vinyl (Rec(RNil), RecApplicative(rpure), rmap, rapply)
import Data.Vinyl.TypeLevel 
import Data.Vinyl.Functor (Lift(..), Identity(..))
import Control.Lens (view, (&), (?~))
import Pipes (Producer, (>->), runEffect)
import Data.Monoid ((<>),First(..))
import Data.Maybe (fromMaybe, isNothing)
import qualified Control.Foldl as L
import qualified Pipes.Prelude as P
import qualified Data.Text as T
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
                  ) =>
                  Record rs -> Record rs2
mkCompositeKey' denormRow = fromMaybe (error "failed to make composite key") . recMaybe $ newRow
  where newRow = rpure Nothing & compositeKey' ?~ Col compositeKeyTxt
        compositeKeyTxt = view' keyA <> view' keyB <> view' keyC <> view' keyD
        view' l = LT.toStrict $ T.format "{}" (T.Only (view l denormRow))
        intToTxt = T.pack . show :: Int -> Text -- slow!

mkCompositeKey :: Text -> Text -> Text -> Text -> Text
mkCompositeKey a b c d = a <> b <> c <> d

defaultingProducer :: ( ReadRec rs
                      , RecApplicative rs
                      , MonadIO m
                      , LAll Default rs
                      ) =>
                      FilePath -> String -> Producer (Rec Identity rs) m ()
defaultingProducer fp label = readTableMaybe fp >-> P.map (holeFiller label)

recMaybe'' :: forall rs. Rec Maybe rs -> Maybe (Record rs)
recMaybe'' = recMaybe

holeFiller :: forall ty.
              ( LAll Default ty
              , RecApplicative ty
              ) =>
              String -> Rec Maybe ty -> Rec Identity ty
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
  where compositeKeyTxt = mkCompositeKey (viewToTxt keyA inRecord) (viewToTxt keyB inRecord) (viewToTxt keyC inRecord) (viewToTxt keyD inRecord)
        viewToTxt l r = intToTxt (view l r)
        intToTxt i = LT.toStrict $ T.format "{}" (T.Only i)

addCompositeKeyToMapFold :: ( Num a
                            , CompositeKey ∈ rs
                            ) => L.Fold (Record rs) (M.Map Text a)
addCompositeKeyToMapFold = L.Fold (\m r -> addCompositeKeyToMap m r) (M.empty) id

addCompositeKeyToMap :: ( Num a
                        , CompositeKey ∈ rs
                        ) => M.Map Text a -> Record rs -> M.Map Text a
addCompositeKeyToMap m r = M.insert (view compositeKey r) 0 m

buildCompositeKeyMap :: ( Foldable f
                        , CompositeKey ∈ rs
                        ) => f (Record rs) -> M.Map Text Integer
buildCompositeKeyMap = L.fold addCompositeKeyToMapFold

findMissingRows :: ( Monad m,
                     CompositeKey ∈ rs,
                     CompositeKey ∈ rs1,
                     L.PrimMonad pm,
                     Frames.InCore.RecVec rs
                   ) =>
                   Producer (Record rs1) m r
                -> Producer (Record rs) pm ()
                -> pm (Producer (Record rs1) m r)
findMissingRows referenceProducer checkProducer = do
  -- build the index of rows in the producer to check
  compositeKeyMap <- buildCompositeKeyMap <$> inCoreAoS checkProducer
  -- we could have built compositeKeyMap in a single line if we were golfing
  -- L.fold (L.Fold (\m r -> M.insert (view compositeKey r) 0 m) M.empty id) <$> inCoreAoS checkProducer

  -- keep only rows we didn't have in the checkProducer index produced
  return $ referenceProducer >-> P.filter (\r -> isNothing $ M.lookup (view compositeKey r) compositeKeyMap)

printMissingRows :: IO ()
printMissingRows = do
  putStrLn "rows normalized contains deonrmalized does not"
  -- findMissingRows normalized denormalized' >>= \p -> runEffect $ p >-> P.print
  -- TODO this needs to actually return the right answer :P
  findMissingRows normalized denormalized >>= \p -> runEffect $ p >-> P.print

main = undefined

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
module Main where

import Frames hiding ((:&))
import Frames.CSV (tableTypesOverride, RowGen(..), rowGen, readTableOpt)
import Frames.InCore (RecVec)
import Data.Vinyl (Rec(RNil), RecApplicative(rpure), rmap, rapply)
import Data.Vinyl.TypeLevel (RIndex)
import Data.Vinyl.Functor (Lift(..))
import Control.Lens (view, (&), (?~))
import Pipes (Producer, (>->), runEffect)
import Pipes.Internal (Proxy)
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

instance Default (s :-> Int) where def = 0
instance Default (s :-> Text) where def = mempty

-- We can write instances for /all/ 'Rec' values.
instance (Applicative f, LAll Default ts, RecApplicative ts)
  => Default (Rec f ts) where
  def = reifyDict [pr|Default|] (pure def)

mkCompositeKey :: (RecApplicative rs2, KeyA ∈ rs, KeyB ∈ rs, KeyC ∈ rs, KeyD ∈ rs, CompositeKey ∈ rs2) =>
                  Record rs -> Record rs2
mkCompositeKey denormRow = fromMaybe (error "failed to make composite key") . recMaybe $ newRow
  where newRow = rpure Nothing & compositeKey' ?~ Col compositeKeyTxt
        compositeKeyTxt = view' keyA <> view' keyB <> view' keyC <> view' keyD
        view' l = LT.toStrict $ T.format "{}" (T.Only (view l denormRow))
        intToTxt = T.pack . show :: Int -> Text -- slow!

normalizedDefaulted :: Producer Normalized IO ()
normalizedDefaulted = readTableMaybe "normalized.csv" >-> P.map (fromJust . holeFiller)
  where holeFiller :: Rec Maybe (RecordColumns Normalized) -> Maybe Normalized
        holeFiller = recMaybe
                   . rmap getFirst
                   . rapply (rmap (Lift . flip (<>)) def)
                   . rmap First
        fromJust = fromMaybe (error "normalizedDefaulted failure")

denormalizedDefaulted :: Producer Denormalized IO ()
denormalizedDefaulted = readTableMaybe "denormalized.csv" >-> P.map (fromJust . holeFiller)
  where holeFiller :: Rec Maybe (RecordColumns Denormalized) -> Maybe Denormalized
        holeFiller = recMaybe
                   . rmap getFirst
                   . rapply (rmap (Lift . flip (<>)) def)
                   . rmap First
        fromJust = fromMaybe (error "denormalizedDefaulted failure")

-- the below example shows why denormalizedDefaulted is necessary in combination with the previous commit message headline and body:
-- λ> runEffect $ denormalized >-> P.print
-- {key_a :-> 1, key_b :-> 2, key_c :-> 3, key_d :-> 4, tag :-> "one"}
-- {key_a :-> 1, key_b :-> 1, key_c :-> 2, key_d :-> 1, tag :-> "four"}
-- λ> runEffect $ denormalizedDefaulted >-> P.print
-- {key_a :-> 1, key_b :-> 2, key_c :-> 3, key_d :-> 4, tag :-> "one"}
-- {key_a :-> 9, key_b :-> 1, key_c :-> 0, key_d :-> 1, tag :-> "three"}
-- {key_a :-> 1, key_b :-> 1, key_c :-> 2, key_d :-> 1, tag :-> "four"}


-- denormalized' is actually the "normalized" denormalized in that it contains the same compositeKey. I was lazy and didn't care about keeping the tag.
denormalizedDefaulted' :: Producer (Record '[CompositeKey]) IO ()
denormalizedDefaulted' = denormalizedDefaulted >-> P.map mkCompositeKey

normalized :: Producer Normalized IO ()
normalized = readTableOpt normalizedParser "normalized.csv"
-- λ> runEffect $ normalized >-> P.take 5 >-> P.print
-- {composite_key :-> "1234", tag :-> "one"}
-- {composite_key :-> "5678", tag :-> "two"}
-- {composite_key :-> "9101", tag :-> "three"}

denormalized :: Producer Denormalized IO ()
denormalized = readTableOpt denormalizedParser "denormalized.csv"
-- λ> runEffect $ denormalized >-> P.take 5 >-> P.print
-- {key_a :-> 1, key_b :-> 2, key_c :-> 3, key_d :-> 4, tag :-> "one"}
-- {key_a :-> 5, key_b :-> 6, key_c :-> 7, key_d :-> 8, tag :-> "two"}
-- {key_a :-> 9, key_b :-> 1, key_c :-> 0, key_d :-> 1, tag :-> "three"}

-- denormalized' is actually the "normalized" denormalized in that it contains the same compositeKey. I was lazy and didn't care about keeping the tag.
denormalized' :: Producer (Record '[CompositeKey]) IO ()
denormalized' = denormalized >-> P.map mkCompositeKey
-- λ> runEffect $ denormalized' >-> P.take 5 >-> P.print
-- {composite_key :-> "1234"}
-- {composite_key :-> "5678"}
-- {composite_key :-> "9101"}

addCompositeKeyToMapFold :: (Num a, CompositeKey ∈ rs) =>
                            L.Fold (Record rs) (M.Map Text a)
addCompositeKeyToMapFold = L.Fold (\m r -> addCompositeKeyToMap m r) (M.empty) id

addCompositeKeyToMap
  :: (Num a, CompositeKey ∈ rs) =>
     M.Map Text a -> Record rs -> M.Map Text a
addCompositeKeyToMap m r = M.insert (view compositeKey r) 0 m

buildCompositeKeyMap :: (Foldable f, CompositeKey ∈ rs) =>
                        f (Record rs) -> M.Map Text Integer
buildCompositeKeyMap = L.fold addCompositeKeyToMapFold

findMissingRows :: forall
                   (m :: * -> *)
                   (rs :: [*])
                   a'
                   a
                   (m1 :: * -> *)
                   r
                   (rs1 :: [*]).
                   ( Monad m1,
                     CompositeKey ∈ rs,
                     CompositeKey ∈ rs1,
                     L.PrimMonad m,
                     Frames.InCore.RecVec rs
                   ) =>
                   Pipes.Internal.Proxy a' a () (Record rs1) m1 r
                -> Producer (Record rs) m ()
                -> m (Pipes.Internal.Proxy a' a () (Record rs1) m1 r)
findMissingRows referenceProducer checkProducer = do
  -- build the index of rows in the producer to check
  compositeKeyMap <- buildCompositeKeyMap <$> inCoreAoS checkProducer
  -- we could have built compositeKeyMap in a single line if we were golfing
  -- L.fold (L.Fold (\m r -> M.insert (view compositeKey r) 0 m) (M.empty) id) <$> inCoreAoS checkProducer

  -- keep only rows we didn't have in the checkProducer index produced
  return $ referenceProducer >-> P.filter (\r -> isNothing $ M.lookup (view compositeKey r) compositeKeyMap)

printMissingRows :: IO ()
printMissingRows = do
  putStrLn "rows normalized contains deonrmalized does not"
  -- findMissingRows normalized denormalized' >>= \p -> runEffect $ p >-> P.print
  findMissingRows normalized denormalizedDefaulted' >>= \p -> runEffect $ p >-> P.print

main = undefined

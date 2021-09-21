-- 01. Print the names of trainers that have caught 3 or more Pokémon in the ascending order of the number of Pokémon caught.
SELECT tname 
FROM (
  SELECT Trainer.name AS 'tname', Trainer.id, COUNT(*) AS 'pokemonnum' 
  FROM Trainer, CatchedPokemon 
  WHERE Trainer.id = CatchedPokemon.owner_id 
  GROUP BY Trainer.id
) A
WHERE pokemonnum >= 3
ORDER BY pokemonnum;
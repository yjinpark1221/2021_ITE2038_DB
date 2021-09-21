-- 32. Print the name of the trainer who caught only one type(e.g. water, fire) of Pokémon, the name of the Pokémon, and the number of times caught, in alphabetical order of the trainer name.
SELECT Trainer.name, Pokemon.name, COUNT(*) 
FROM Trainer, CatchedPokemon, Pokemon
WHERE Trainer.id = CatchedPokemon.owner_id AND CatchedPokemon.pid = Pokemon.id AND Trainer.id IN (
  SELECT id 
  FROM (
    SELECT DISTINCT Trainer.id, Pokemon.type
    FROM Trainer, CatchedPokemon, Pokemon
    WHERE Trainer.id = CatchedPokemon.owner_id AND CatchedPokemon.pid = Pokemon.id
  ) AS A
  GROUP BY id
  HAVING COUNT(*) = 1
)
GROUP BY Pokemon.id, Trainer.id
ORDER BY Trainer.name;
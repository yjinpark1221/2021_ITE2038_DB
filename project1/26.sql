-- 26. Print the name of the trainer with the highest total of the Pokémon levels caught and the total number of levels.
SELECT name, total_level
FROM Trainer, (
  SELECT owner_id AS 'id', SUM(level) AS 'total_level'
  FROM CatchedPokemon
  GROUP BY owner_id
) AS TrainerLevel
WHERE Trainer.id = TrainerLevel.id AND total_level = (
  SELECT SUM(level) AS 'total_level'
  FROM CatchedPokemon
  GROUP BY owner_id
  ORDER BY SUM(level) DESC LIMIT 1
);
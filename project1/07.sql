-- 07. For each city of origin, print the nickname of the Pokémon with the highest level among the Pokémon caught by trainers in the same city of origin in alphabetical order(If there is no trainer in the city, omit the information of that city. If there is more than one highest level, just print all cases.).
SELECT nickname
FROM (
  SELECT hometown, MAX(level) AS 'max_level'
  FROM Trainer, CatchedPokemon
  WHERE Trainer.id = CatchedPokemon.owner_id
  GROUP BY Trainer.hometown
) AS Hometown, Trainer, CatchedPokemon
WHERE Trainer.hometown = Hometown.hometown AND CatchedPokemon.owner_id = Trainer.id AND CatchedPokemon.level = Hometown.max_level
ORDER BY nickname;
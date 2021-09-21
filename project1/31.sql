-- 31. Print the names of Pokémon caught in common by trainers from Sangnok city and trainers from Blue city in alphabetical order.
Select name 
FROM (
  SELECT pid
  FROM CatchedPokemon, Trainer
  WHERE Trainer.id = CatchedPokemon.owner_id AND Trainer.hometown = 'Sangnok City'
) AS Sangnok, (
  SELECT pid
  FROM CatchedPokemon, Trainer
  WHERE Trainer.id = CatchedPokemon.owner_id AND Trainer.hometown = 'Blue City'
) AS Blue, Pokemon
WHERE Sangnok.pid = Blue.pid AND Pokemon.id = Sangnok.pid
ORDER BY name;
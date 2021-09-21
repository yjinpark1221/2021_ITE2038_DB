-- 09. Print the name of the trainer who caught 4 or more Pokémon and the nickname of the Pokémon with the highest level among the Pokémon caught by that trainer in alphabetical order of the Pokémon nicknames. (If there is more than one highest level, just print all cases.)
SELECT Trainer.name, CatchedPokemon.nickname
FROM (
  SELECT owner_id, MAX(level) AS max_level, COUNT(*) AS num_caught
  FROM CatchedPokemon
  GROUP BY owner_id
) AS Link, Trainer, CatchedPokemon
WHERE Link.owner_id = Trainer.id AND Link.max_level = CatchedPokemon.level AND Link.num_caught >= 4 AND CatchedPokemon.owner_id = Trainer.id
ORDER BY nickname;
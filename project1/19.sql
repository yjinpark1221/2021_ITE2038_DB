-- 19. Print the total level of Pokémon caught by Trainer Matis.
SELECT SUM(level)
FROM CatchedPokemon, Trainer
WHERE CatchedPokemon.owner_id = Trainer.id AND Trainer.name = 'Matis';
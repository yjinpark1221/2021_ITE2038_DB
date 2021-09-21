-- 06. Print the name of the gym leader and the average of the Pokémon level caught by each leader in alphabetical order of the leader's name.
SELECT Trainer.name, AVG(level)
FROM Trainer, Gym, CatchedPokemon
WHERE Trainer.id = Gym.leader_id AND Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.name
ORDER BY Trainer.name;
-- 40. Print the names of the trainers who caught Pikachu in alphabetical order.
SELECT Trainer.name
FROM Trainer, Pokemon, CatchedPokemon
WHERE Trainer.id = CatchedPokemon.owner_id AND Pokemon.id = CatchedPokemon.pid AND Pokemon.name = 'Pikachu'
ORDER BY Trainer.name;
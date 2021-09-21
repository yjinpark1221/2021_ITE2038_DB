-- 21. Print the names of the trainers who caught two or more of the same Pokémon in alphabetical order.
SELECT name
FROM Trainer, CatchedPokemon
WHERE Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.name, CatchedPokemon.pid
HAVING COUNT(*) >= 2;
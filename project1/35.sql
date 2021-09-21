-- 35. Print the trainer's name and the number of Pokémon caught by that trainer in alphabetical order by the trainer's name.
SELECT Trainer.name, COUNT(*)
FROM Trainer, CatchedPokemon
WHERE Trainer.id = CatchedPokemon.owner_id
GROUP BY Trainer.id
ORDER BY Trainer.name;
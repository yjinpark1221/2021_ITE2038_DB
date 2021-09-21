-- 34. Print the Pokémon's name after the Pokémon with the name Charmander has evolved twice.
SELECT Evolved.name
FROM Pokemon AS Original, Evolution AS First, Evolution AS Second, Pokemon AS Evolved
WHERE Original.id = First.before_id AND First.after_id = Second.before_id AND Second.after_id = Evolved.id AND Original.name = 'Charmander';
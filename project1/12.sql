-- 12. Print the pre-evolution names of the Pokémon whose ids decrease as they evolve in alphabetical order.
SELECT Pokemon.name
FROM Pokemon, Evolution
WHERE Pokemon.id = Evolution.before_id AND Evolution.before_id > Evolution.after_id
ORDER BY Pokemon.name;
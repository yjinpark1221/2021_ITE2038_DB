-- 25. Print the names of the evolutionary stage 2 Pokémon in alphabetical order. (ex. In the case of SquirtleBuggy-Turtle King, Buggy ,which is the Pokémon with stage 2 evolution, must be printed out, and Pokémon whose stage 2 evolution is the final evolution form also must also be printed.)
SELECT name
FROM Pokemon, Evolution
WHERE Evolution.after_id = Pokemon.id AND Evolution.before_id NOT IN (
  SELECT after_id
  FROM Evolution
)
ORDER BY name;
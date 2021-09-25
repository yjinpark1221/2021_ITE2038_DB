-- 36. Print out the Pokemon whose type is water, which cannot be evolved any more, in alphabetical order. (In the case of a Pokémon that has undergone previous evolution, if it cannot evolve in the future, it is judged as a Pokémon that cannot evolve anymore. ex. In the case of Squirtle-Buggy-Turtle King, Turtle King is a Pokémon that can no longer evolve.)
SELECT name
FROM Pokemon
WHERE id NOT IN (
  SELECT before_id
  FROM Evolution
) AND type = 'Water' AND id IN (
  SELECT after_id
  FROM Evolution 
)
ORDER BY name;
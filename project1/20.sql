-- 20. Print the names of the gym leaders in alphabetical order.
SELECT name
FROM Trainer, Gym
WHERE Trainer.id = Gym.leader_id
ORDER BY name;
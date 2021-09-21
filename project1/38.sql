-- 38. Print the name of the gym leader in Brown city.
SELECT name
FROM Gym, Trainer
WHERE Trainer.id = Gym.leader_id AND Gym.city = 'Brown City';
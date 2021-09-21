-- 16. Print the name of the city's gym leader with Amazon characteristics.
SELECT Trainer.name
FROM Trainer, Gym, City
WHERE Gym.leader_id = Trainer.id AND City.name = Gym.city AND City.description = 'Amazon';
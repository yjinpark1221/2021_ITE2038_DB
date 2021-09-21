-- 03. Print the name of the trainer, not the gym leader, in alphabetical order.
SELECT Trainer.name
FROM Trainer
WHERE Trainer.id NOT IN (SELECT leader_id FROM Gym)
ORDER BY Trainer.name;
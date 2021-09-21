-- 28. Print the names of trainers from Brown city or Rainbow city in alphabetical order.
SELECT name
FROM Trainer
WHERE hometown = 'Brown City' OR hometown = 'Rainbow City'
ORDER BY name;
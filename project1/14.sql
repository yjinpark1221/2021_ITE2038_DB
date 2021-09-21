-- 14. Print the names of the cities with the most native trainers.
SELECT hometown
FROM (
  SELECT hometown, COUNT(*) AS count
  FROM Trainer
  GROUP BY hometown
) AS CityLink
WHERE CityLink.count = (SELECT COUNT(*) FROM Trainer GROUP BY hometown ORDER BY COUNT(*) DESC LIMIT 1);
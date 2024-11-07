SELECT * FROM dbo.raw_data2
SELECT * FROM dbo.raw_data
-- Поиск и удаление дубликатов по URL в данных

SELECT * 
FROM 
	(SELECT *,ROW_NUMBER() OVER(PARTITION BY url ORDER BY (SELECT NULL)) AS row_num FROM dbo.raw_data ) AS a
WHERE 
	row_num > 1;

WITH temp_raw_date AS(
SELECT  [url]
      ,[title]
      ,[property type]
      ,[property size]
      ,[parking]
      ,[lift]
      ,[floor]
      ,[price]
      ,[service_charge]
      ,[year built]
      ,[building registration type]
      ,[preferred tennant]
      ,[interior]
      ,[garage size]
      ,[front road size]
      ,[common area]
      ,[bedrooms]
      ,[bathrooms]
      ,[location]
      ,[country],
	  ROW_NUMBER() OVER(PARTITION BY url ORDER BY url) AS row_num
  FROM [Data_Cleaning_Project].[dbo].[raw_data]
)
DELETE
FROM temp_raw_date
WHERE
	row_num > 1;

-- Очистка и нормализация данных о цене недвижимости
SELECT 
	price,
	CASE	
		WHEN LOWER(price) LIKE '%cr%' THEN  CAST(SUBSTRING(LTRIM(price), 1, CHARINDEX('cr', LOWER(price)) - 1) AS BIGINT) * 10000000
		WHEN LOWER(price) LIKE '%lac%' THEN  CAST(SUBSTRING(LTRIM(price), 1, CHARINDEX('lac', LOWER(price)) - 1) AS BIGINT) * 100000
		ELSE price
	END
FROM 
	dbo.raw_data;

UPDATE dbo.raw_data
SET price = CASE	
		WHEN LOWER(price) LIKE '%cr%' THEN  CAST(SUBSTRING(LTRIM(price), 1, CHARINDEX('cr', LOWER(price)) - 1) AS BIGINT) * 10000000
		WHEN LOWER(price) LIKE '%lac%' THEN  CAST(SUBSTRING(LTRIM(price), 1, CHARINDEX('lac', LOWER(price)) - 1) AS BIGINT) * 100000
		ELSE price
	END;

SELECT 
	price,
	CASE
		WHEN CHARINDEX('/' ,price) > 0 THEN TRIM(SUBSTRING(price ,1 ,CHARINDEX('/' ,price)-1))
		ELSE price
	END step_1,
	CASE
		WHEN CHARINDEX('(' ,price) > 0 THEN TRIM(SUBSTRING(price ,1 ,CHARINDEX('(' ,price)-1))
		ELSE price
	END step_2,
	REPLACE(price,',' ,'')
FROM 
	dbo.raw_data;

UPDATE dbo.raw_data
SET price = CASE
		WHEN CHARINDEX('/' ,price)-1 > 0 THEN SUBSTRING(price ,1,CHARINDEX('/' ,price)-1)
		WHEN CHARINDEX('(' ,price)-1 > 0 THEN SUBSTRING(price ,1,CHARINDEX('(' ,price)-1)
		ELSE price
	END
FROM
	dbo.raw_data;

UPDATE dbo.raw_data
SET price = REPLACE(REPLACE(price, ',', ''), '.', '');

ALTER TABLE dbo.raw_data
ALTER COLUMN price BIGINT;

ALTER TABLE dbo.raw_data
ALTER COLUMN lift INT;

--Парсинг и распределение данных по столбцам: area, city, postal_code

ALTER TABLE dbo.raw_data
ADD area NVARCHAR(255);

ALTER TABLE dbo.raw_data
ADD city NVARCHAR(255);

ALTER TABLE dbo.raw_data
ADD postal_code NVARCHAR(255);


WITH temp_result AS (
SELECT 
	location,
    CASE 
        WHEN CHARINDEX(',', location) > 0 
        THEN LTRIM(RTRIM(SUBSTRING(location, 1, CHARINDEX(',', location) + 1))) 
        ELSE location 
    END AS area,      
	CASE 
		 WHEN CHARINDEX(',', location) > 0 THEN 
		 LTRIM(RTRIM(SUBSTRING(location, CHARINDEX(',', location) + 1, LEN(location) - CHARINDEX(',', location))))
		 ELSE location 
	END  AS City,
	CASE 
        WHEN location LIKE '%[0-9][0-9][0-9][0-9]%' THEN
        SUBSTRING(location, PATINDEX('%[0-9][0-9][0-9][0-9]%', location), 4)
        ELSE NULL
        END AS postal_code
FROM 
	dbo.raw_data
),
temp_result2 AS(
SELECT 
	location,
	CASE
    WHEN CHARINDEX(' ', REPLACE(area, ',', '')) > 0 
	THEN
		SUBSTRING(REPLACE(area, ',', ''), 1, CHARINDEX(' ', REPLACE(area, ',', '')) - 1)
    ELSE
		REPLACE(area, ',', '')
END AS area,

	CASE 
        WHEN LOWER(location) LIKE '%dhaka%' THEN 'Dhaka' 
        ELSE 'Dhaka' 
    END AS city,
	 postal_code
FROM 
	temp_result
)
UPDATE rw
SET rw.area = tr.area,
	rw.city = tr.city,
    rw.postal_code = tr.postal_code
FROM 
	DBO.raw_data rw
JOIN
	temp_result2 tr ON rw.location = tr.location;

-- Очистка и нормализация значений в столбце 'front road size

SELECT
    [front road size],
    CASE
        WHEN TRIM([front road size]) LIKE '%ft%' OR TRIM([front road size]) LIKE '%ft.%'
        THEN TRIM(REPLACE(REPLACE([front road size],'ft' ,'') ,'.' ,''))
        ELSE TRIM([front road size])
    END AS [front road size/Ft]
FROM
    dbo.raw_data;

UPDATE dbo.raw_data
SET [front road size] =CASE WHEN TRIM([front road size]) LIKE '%ft%' OR TRIM([front road size]) LIKE '%ft.%'
        THEN TRIM(REPLACE(REPLACE([front road size],'ft' ,'') ,'.' ,''))
        ELSE TRIM([front road size])
    END 
FROM
    dbo.raw_data;

-- Очистка и нормализация значений в столбце 'garage_sq_ft

SELECT
	garage_sq_ft,
    CASE
        WHEN CHARINDEX(' ', garage_sq_ft) > 0
        THEN REPLACE(SUBSTRING(LOWER(garage_sq_ft), 1, CHARINDEX(' ', lower(garage_sq_ft)) - 1), 'В', '')
        ELSE garage_sq_ft
    END AS cleaned_garage_size
FROM
    dbo.raw_data;

UPDATE dbo.raw_data
SET garage_sq_ft = CASE WHEN CHARINDEX(' ', garage_sq_ft) > 0
				   THEN REPLACE(SUBSTRING(LOWER(garage_sq_ft), 1, CHARINDEX(' ', lower(garage_sq_ft)) - 1), 'В', '')
				   ELSE garage_sq_ft
				   END 
FROM
    dbo.raw_data;

--Переименование столбцов 'front road size' и 'garage size

EXEC sp_rename 'dbo.raw_data.[front road size]', 'front_road_ft', 'COLUMN';
EXEC sp_rename 'dbo.raw_data.[garage size]', 'garage_sq_ft', 'COLUMN';

-- Очистка и нормализация данных по размеру гаража, добавление столбца для наличия гаража

SELECT 
    CASE
        WHEN LOWER(garage_sq_ft) LIKE '%sq%' 
        THEN REPLACE(lower(garage_sq_ft) ,'sq', '')
		WHEN LOWER(garage_sq_ft) LIKE '%n/a%'
		THEN REPLACE(lower(garage_sq_ft) ,'n/a',NULL)
        ELSE TRIM(garage_sq_ft)
    END AS cleaned_garage_size
FROM 
    dbo.raw_data

UPDATE dbo.raw_data
SET garage_sq_ft = CASE
    WHEN LOWER(garage_sq_ft) LIKE '%sq%' THEN 
        REPLACE(LOWER(garage_sq_ft), 'sq', '')
    WHEN LOWER(garage_sq_ft) LIKE '%n/a%' THEN 
        REPLACE(LOWER(garage_sq_ft), 'n/a', NULL)
    ELSE 
        TRIM(garage_sq_ft)
END;

ALTER TABLE dbo.raw_data
ADD has_garage CHAR (1);

UPDATE dbo.raw_data
SET has_garage = CASE
		WHEN garage_sq_ft IS NOT NULL  THEN 'Y'
		ELSE 'N'
	END
FROM 
	dbo.raw_data;

UPDATE dbo.raw_data
SET garage_sq_ft = '0'
WHERE garage_sq_ft IS NULL;

ALTER TABLE dbo.raw_data
ALTER COLUMN garage_sq_ft INT;

-- Очистка и преобразование размера недвижимости в числовой формат

SELECT DISTINCT
    [property size],
    CASE
        WHEN LOWER([property size]) LIKE '%sq%' THEN
            LTRIM(REPLACE(
                REPLACE(
                    REPLACE(
                        SUBSTRING(LOWER([property size]), 1, CHARINDEX(' ', LOWER([property size])) - 1), 
                        'В', ''), 
                    'sq', ''), 
                ',', '')
            )
        ELSE [property size] 
    END AS processed_property_size
FROM 
    dbo.raw_data;

UPDATE dbo.raw_data
SET [property size] = 
    CASE
        WHEN LOWER([property size]) LIKE '%sq%' THEN
            LTRIM(REPLACE(
                REPLACE(
                    REPLACE(
                        SUBSTRING(LOWER([property size]), 1, CHARINDEX(' ', LOWER([property size])) - 1), 
                        'В', ''), 
                    'sq', ''), 
                ',', '') 
            )
        ELSE [property size] 
    END;

UPDATE dbo.raw_data
SET [property size] = REPLACE(REPLACE([property size], ' ', ''), CHAR(160), '')
WHERE [property size] IS NOT NULL;

ALTER TABLE raw_data
ALTER COLUMN [property size] INT;

-- Очистка и преобразование общей площади в числовой формат

SELECT DISTINCT
	[common area],
	CASE	
		WHEN CHARINDEX(' ' ,[common area]) > 0
		THEN REPLACE(LTRIM(SUBSTRING([common area] ,1 ,CHARINDEX(' ' ,[common area])-1)),'В' ,'')
		ELSE [common area]
	END
FROM dbo.raw_data

UPDATE raw_data
SET [common area] = CASE	
		WHEN CHARINDEX(' ' ,[common area]) > 0
		THEN REPLACE(LTRIM(SUBSTRING([common area] ,1 ,CHARINDEX(' ' ,[common area])-1)),'В' ,'')
		ELSE [common area]
	END;

UPDATE dbo.raw_data
SET [common area] = REPLACE(REPLACE([common area] ,' ' ,''),CHAR(160) ,'')
WHERE 
	[common area] IS NOT NULL;

ALTER TABLE dbo.raw_data
ALTER COLUMN [common area] INT;

-- Очистка и преобразование количества комнат и ванных в числовой формат

SELECT DISTINCT
    bedrooms,
    CASE
		WHEN bedrooms = '03 Bedrooms+ 01 Study Room'
		THEN '4'
        WHEN CHARINDEX(' ', bedrooms) > 0
        THEN LTRIM(SUBSTRING(bedrooms, 1, CHARINDEX(' ', bedrooms) - 1))
        ELSE bedrooms
    END AS processed_bedrooms,
    bathrooms,
    CASE
        WHEN CHARINDEX(' ', bathrooms) > 0
        THEN LTRIM(SUBSTRING(bathrooms, 1, CHARINDEX(' ', bathrooms) - 1)) 
        ELSE bathrooms
    END AS processed_bathrooms
FROM 
    dbo.raw_data;

UPDATE dbo.raw_data
SET bedrooms = CASE
		WHEN bedrooms = '03 Bedrooms+ 01 Study Room'
		THEN '4'
        WHEN CHARINDEX(' ', bedrooms) > 0
        THEN LTRIM(SUBSTRING(bedrooms, 1, CHARINDEX(' ', bedrooms) - 1))
        ELSE bedrooms
    END,
	bathrooms = CASE
        WHEN CHARINDEX(' ', bathrooms) > 0
        THEN LTRIM(SUBSTRING(bathrooms, 1, CHARINDEX(' ', bathrooms) - 1)) 
        ELSE bathrooms
    END;

ALTER TABLE raw_data
ALTER COLUMN bedrooms INT;

ALTER TABLE raw_data
ALTER COLUMN bathrooms INT;

-- Удаление лишних пробелов и символов (CHAR(160))

UPDATE dbo.raw_data
SET interior = REPLACE(REPLACE(interior ,' ' ,''),CHAR(160) ,'');

-- Стандартизация значений

UPDATE dbo.raw_data
SET interior = CASE 
	WHEN interior = 'Unfurnished'
	THEN 'Unfurnished'
    WHEN interior = 'Un' THEN 'UnFurnished'
	WHEN interior = 'FullFurnished' THEN 'Fully Furnished'
	WHEN interior = 'NonFurnished' THEN 'Unfurnished'
	WHEN interior = 'SemiUnfurnished' THEN 'Semi-Furnished'
	WHEN interior = 'SemiFurnished' THEN 'Semi-Furnished'
	WHEN interior IS NULL
	THEN 'UnFurnished'
    ELSE interior 
END
;

-- Очистка и нормализация данных о стоимости обслуживания: удаление нерелевантных значений в столбце service_charge

UPDATE dbo.raw_data
SET service_charge = NULL
WHERE 
	LOWER(service_charge) LIKE '%bd%' 
	OR 
	LOWER(service_charge) LIKE '%bdt%'
	OR 
	LOWER(service_charge) LIKE '%/-%' 
	OR
	LOWER(service_charge) IN (' At Actual',' At Actual.',' N/A',' Not Fixed yet',' Not fixed yet.','At Actual.');

-- Добавление нового столбца 'service_charge_status' для хранения информации о статусе включения стоимости обслуживания в аренду

ALTER TABLE dbo.raw_data
ADD service_charge_status NVARCHAR(255);

UPDATE dbo.raw_data
SET service_charge = CASE 
        WHEN LOWER(service_charge) LIKE '%inclu%' THEN 'Included with Rent'
        ELSE 'Not Included'
    END;

UPDATE dbo.raw_data
SET service_charge_status = service_charge;

UPDATE dbo.raw_data
SET service_charge = CASE
		WHEN service_charge IS NULL
		THEN ([property size] * 10) 
		ELSE NULL
	END;

-- Обновление и нормализация данных: изменение типа столбца 'year built' на DATE после добавления фиксированного дня (01-01),
-- удаление лишних пробелов в столбцах 'url', 'title', 'property type' и 'building registration type'

ALTER TABLE dbo.raw_data
ALTER COLUMN [year built] NVARCHAR(255);

UPDATE dbo.raw_data
SET [year built] = CONCAT([year built],'-01-01')
WHERE 
	[year built] IS NOT NULL;

ALTER TABLE dbo.raw_data
ALTER COLUMN [year built] DATE;

UPDATE dbo.raw_data
SET url = LTRIM(url),
	title = LTRIM(title),
	[property type] = LTRIM(url),
	[building registration type] = LTRIM([building registration type])

-- Создание резервной копии данных таблицы raw_data

SELECT *
INTO dbo.raw_data_backup
FROM dbo.raw_data;
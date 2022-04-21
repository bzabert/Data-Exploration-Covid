import pandas as pd
import pandasql as ps


df_1 = pd.read_csv("/Users/bzabert/Documents/SQL/Portfolio/owid-covid-data-death.csv")
df_2 = pd.read_csv(
    "/Users/bzabert/Documents/SQL/Portfolio/owid-covid-data-vaccinations.csv"
)


# print(df_1.columns)
# print(df_2.columns)

# Fisrt look at the Data Frame
query_0 = """SELECT location, date, total_deaths, total_cases, new_cases 
            FROM df_1   
            GROUP BY 1,2 """

# Evolutuion in time of Death Percentage for Argentina
query_1 = """SELECT location, date, total_deaths, total_cases, new_cases, (total_deaths/total_cases)*100 AS PercentOfDeath 
            FROM df_1 
            WHERE location IS 'Argentina' 
            GROUP BY 2 """

# Evolutuion in time of Infection Percentage for Argentina
query_2 = """SELECT location, date, population, total_cases, new_cases, (total_cases/population)*100 AS PercentOfcases 
            FROM df_1 
            WHERE location IS 'Argentina' 
            GROUP BY 2 """

# Infection Percentage by Country
query_3 = """SELECT location, population, MAX(total_cases), new_cases, (MAX(total_cases)/population)*100 AS PercentOfcases 
            FROM df_1
            GROUP BY location 
            ORDER BY 5 DESC"""

# Death Percentage by Country
query_4 = """SELECT location, continent, MAX(total_cases), MAX(total_deaths), (MAX(total_deaths)/MAX(total_cases))*100 AS PercentOfDeaths 
            FROM df_1 
            WHERE continent IS NOT NULL GROUP BY location 
            ORDER BY PercentOfDeaths DESC"""

# Death Percentage by Continent
query_5 = """SELECT  location, MAX(total_cases) AS Highest_Cases_Count, MAX(total_deaths) AS Highest_Deaths_Count, (MAX(total_deaths)/MAX(total_cases))*100 AS PercentOfDeaths 
            FROM df_1 
            WHERE continent IS NULL AND location IS NOT 'Low income' AND location IS NOT 'Upper middle income' AND location IS NOT 'Lower middle income' AND location IS NOT 'High middle income' 
            GROUP BY location 
            ORDER BY PercentOfDeaths DESC"""

# Death Percentage Global numbers
query_6 = """SELECT  location, MAX(total_cases) AS Highest_Cases_Count, MAX(total_deaths) AS Highest_Deaths_Count, (MAX(total_deaths)/MAX(total_cases))*100 AS PercentOfDeaths 
            FROM df_1 
            WHERE continent IS NULL AND location IS 'World'"""

# Join two tables vaccionations, cases, deaths globaly
query_7 = """SELECT  df_1.location, MAX(total_cases) AS Highest_Cases_Count, MAX(total_deaths) AS Highest_Deaths_Count, df_2.total_vaccinations 
            FROM df_1 
            JOIN df_2 
            ON df_1.rowid = df_2.rowid  
            WHERE df_1.continent IS NULL AND df_1.location IS 'World'"""

# Join two tables vaccionations, population by location
query_8 = """SELECT df_1.continent, df_1.location, df_1.date, df_1.population, df_2.total_vaccinations 
            FROM df_1 
            JOIN df_2 
            ON df_1.rowid = df_2.rowid 
            WHERE df_1.location IS 'Argentina' 
            ORDER BY 2,3 """

# Calculating the Rolling Vaccionations For Argentina
query_9 = """SELECT df_1.continent, df_1.location, df_1.date, df_2.new_vaccinations, SUM(df_2.new_vaccinations) OVER (PARTITION BY df_1.location ORDER BY df_1.location, df_1.date) AS Total_Vaccionations 
            FROM df_1 
            JOIN df_2 
            ON df_1.rowid = df_2.rowid 
            WHERE df_1.location IS 'Argentina' 
            ORDER BY 2,3 """

# Using CTE the Percentage of the population in Argentina that is vaccianted
query_10 = """WITH CTE AS (SELECT df_1.continent, df_1.location, df_1.population, df_1.date, df_2.new_vaccinations, SUM(df_2.new_vaccinations) OVER (PARTITION BY df_1.location ORDER BY df_1.location, df_1.date) AS Total_Vaccinations 
            FROM df_1 
            JOIN df_2 
            ON df_1.rowid = df_2.rowid 
            WHERE df_1.location IS 'Argentina' 
            ORDER BY 2,3)
            SELECT *, (Total_Vaccinations/population)/100 as PercentageOfVaccionation
            FROM CTE """

print(ps.sqldf(query7, globals()))

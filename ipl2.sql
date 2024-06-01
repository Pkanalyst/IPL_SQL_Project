CREATE DATABASE IPL2;

USE IPL2;

\*1. Create a table named 'matches' with appropriate data types for columns.*/

\*3. Import data from csv file 'IPL_matches.csv'attached in resources to 'matches'.*/

'Added new column with date Datatype'

ALTER TABLE matches
add column Mdate date;

'Converted text string to date'

update matches
set mdate=str_to_date(date,"%m/%d/%Y");

Select * From matches;

\*2. Create a table named 'deliveries' with appropriate data types for columns */
\*4. Import data from csv file 'IPL_Ball.csv' attached in resources to 'matches'.*/

select * from deliveries;

\*5. Select the top 20 rows of the deliveries table.*/

select * from deliveries
limit 20;

\*6. Select the top 20 rows of the matches table*/

select * from matches
limit 20;

\*7. Fetch data of all the matches played on 2nd May 2013.*/

select * from matches
where mdate="2013-05-02";

\*8. Fetch data of all the matches where the margin of victory is more than 100 runs. */

select* from matches
where result_margin>100;

\*9. Fetch data of all the matches where the final scores of both teams tied and order it in descending order of the date.*/ 

select * from matches
where result_margin =0;

\*10. Get the count of cities that have hosted an IPL match.*/

select count(distinct(city)) from matches;

\*11. Create table deliveries_v02 with all the columns of deliveries and an additional column ball_result containing value boundary, 
dot or other depending on the total_run (boundary for >= 4, dot for 0 and other for any other number).*/

select * ,
             case
                   when total_runs >=4 then "Boundary"
                   when total_runs =0 then 0
                   else "Other"
End as Ball_Result
from deliveries;

create table Deliveries_v2(select * ,
             case
                   when total_runs >=4 then "Boundary"
                   when total_runs =0 then 0
                   else "Other"
End as Ball_Result
from deliveries);

select * from deliveries_v2;

\*12. Write a query to fetch the total number of boundaries and dot balls */

select ball_result,count(ball_result)
from Deliveries_v2
group by ball_result;


\*13. Write a query to fetch the total number of boundaries scored by each team */

select BATTING_TEAM AS Teams,count(ball_result) FROM DELIVERIES_V2
where ball_result ="Boundary"
GROUP BY BATTING_TEAM;

\*14. Write a query to fetch the total number of dot balls bowled by each team */

select BATTING_TEAM AS Teams,count(ball_result) FROM DELIVERIES_V2
where ball_result ="0"
GROUP BY BATTING_TEAM;

\*15. Write a query to fetch the total number of dismissals by dismissal kinds */

select dismissal_kind,count(Dismissal_kind)
from deliveries_v2
group by dismissal_kind;


\*16. Write a query to get the top 5 bowlers who conceded maximum extra runs */

select bowler,sum(extra_runs) as Total_Extra_Runs
from deliveries_v2
group by bowler
order by Total_Extra_Runs desc
limit 5;

\*17. Write a query to create a table named deliveries_v03 with all the columns of deliveries_v02 table and two additional column 
(named venue and match_date) of venue and date from table matches. */

select d.id,d.inning,d.overs,d.ball,d.batsman,d.non_striker,d.bowler,d.batsman_runs,d.extra_runs,d.total_runs,d.is_wicket,d.dismissal_kind,
d.player_dismissed,d.fielder,d.extras_type,d.batting_team,d.bowling_team,d.venue,d.Ball_Result,m.venue as NewVenue,M.mDATE AS MATCH_DATE
FROM DELIVERIES_V2 AS D INNER JOIN MATCHES AS M
ON D.ID=M.ID;

CREATE TABLE Deliveries_v3(select d.id,d.inning,d.overs,d.ball,d.batsman,d.non_striker,d.bowler,d.batsman_runs,d.extra_runs,d.total_runs,
d.is_wicket,d.dismissal_kind,d.player_dismissed,d.fielder,d.extras_type,d.batting_team,d.bowling_team,d.venue,d.Ball_Result,m.venue as NewVenue,
M.mDATE AS MATCH_DATE
FROM DELIVERIES_V2 AS D INNER JOIN MATCHES AS M
ON D.ID=M.ID);

select * from deliveries_v3;

\*18. Write a query to fetch the total runs scored for each venue and order it in the descending order of total runs scored. */

select newvenue,sum(total_runs) as Total_Runs_Scored
from deliveries_v3
group by newvenue
order by total_runs_scored desc;

\*19. Write a query to fetch the year-wise total runs scored at Eden Gardens and order it in the descending order of total runs scored */

select year(match_date) as Year,sum(total_runs) as Total_Runs_Scored
from deliveries_v3
where newvenue="eden gardens"
group by Year 
order by total_runs_scored desc;

\*20. Get unique team1 names from the matches table, you will notice that there are two entries for Rising Pune Supergiant one with 
Rising Pune Supergiant and another one with Rising Pune Supergiants. Your task is to create a matches_corrected table with two additional columns 
team1_corr and team2_corr containing team names with replacing Rising Pune Supergiants with Rising Pune Supergiant. Now analyse these newly 
created columns. */

select distinct(team1)
from matches;

create table matches_corrected(select * from matches);

alter table matches_corrected
add column team1_corr varchar(50),
add column team2_corr varchar(50);

select * from matches_corrected;

update matches_corrected
set team1_corr=replace(team1,"Rising Pune Supergiants","Rising Pune Supergiant");

select distinct(team1_corr)
from matches_corrected;

update matches_corrected
set team2_corr=replace(team2,"Rising Pune Supergiants","Rising Pune Supergiant");

select distinct(team2_corr)
from matches_corrected;

\*21. Create a new table deliveries_v04 with the first column as ball_id containing information of match_id, inning, over and ball 
separated by'(For ex. 335982-1-0-1 match_idinning-over-ball) and rest of the columns same as deliveries_v03)' */

select concat(id,"-",inning,"-",overs,"-",ball)as Ball_id
from deliveries_v3;

create table deliveries_v4((select * from deliveries_v3
));

alter table deliveries_v4
add column Ball_id varchar(50);

update deliveries_v4
set Ball_id= concat(id,"-",inning,"-",overs,"-",ball);

select * from deliveries_v4;

\*22. Compare the total count of rows and total count of distinct ball_id in deliveries_v04; */

select count(distinct(ball_id)) as B_id,count(ball_id)
from deliveries_v4
; 

\*23. Create table deliveries_v05 with all columns of deliveries_v04 and an additional column for row number partition over ball_id. 
(HINT : row_number() over (partition by ball_id) as r_num). */

create table Deliveries_v5(select * ,row_number() over(partition by ball_id)as R_num
from deliveries_v4);

select * from deliveries_v5;

\*24. Use the r_num created in deliveries_v05 to identify instances where ball_id is repeating. */

select distinct(r_num) from deliveries_v5;

select * from deliveries_v5
where r_num = 2;

\*25. Use subqueries to fetch data of all the ball_id which are repeating. */
 
select * from deliveries_v5
where ball_id in (select ball_id from deliveries_v5 
where r_num =2);


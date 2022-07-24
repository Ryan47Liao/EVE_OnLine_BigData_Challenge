-- The following code was run in Big Query to proess the data for Graph Analysis
-- Aggregate Winner Data
create table wars.t_winner_aggregate as
select killmail_id,
corporation_id as winner_id,
"Corporation" as winner_type,
sum(damage_done) as damage_done
from `bdpproject-eve.wars.t_attackers`
where corporation_id is not null
and final_blow = true
group by 1,2,3;

-- Aggregate Victim Data
create table wars.t_loser_aggregate as
select killmail_id,
victim_corporation_id as loser_id,
"Corporation" as loser_type,
sum(victim_damage_taken) as damage_taken
from `bdpproject-eve.wars.t_wars`
where victim_corporation_id is not null
group by 1,2,3;

-- Create aggregate table for corporations and their battle statistics (Vertices)
create table wars.corporation_battle_stats as
select
case when winner_id is not null then winner_id else loser_id end as id,
"Corporation" as type,
num_wins + num_losses as tot_battles,
num_wins,
num_losses,
round(num_wins/(num_wins+num_losses),4) as win_perc,
round(num_losses/(num_wins+num_losses),4) as loss_perc,
tot_damage_done,
tot_damage_taken,
round(safe_divide(tot_damage_done,tot_damage_taken),4) as damage_ratio,
round(safe_divide(tot_damage_done,num_wins),4) as damage_done_per_win,
round(safe_divide(tot_damage_taken,num_losses),4) as damage_taken_per_loss,
round(safe_divide((tot_damage_done - tot_damage_taken),(num_wins+num_losses)),4) as net_damage_per_battle
from
(
select a.winner_id, a.winner_type, b.loser_id, b.loser_type, coalesce(a.num_wins,0) as num_wins, 
coalesce(a.tot_damage_done,0) as tot_damage_done, coalesce(b.num_losses,0) as num_losses, 
coalesce(b.tot_damage_taken,0) as tot_damage_taken
from
  (
  select winner_id, winner_type, count(*) as num_wins, sum(damage_done) as tot_damage_done
  from `bdpproject-eve.wars.t_winner_aggregate`
  group by 1,2
  ) a
full join
  (
  select loser_id, loser_type, count(*) as num_losses, sum(damage_taken) as tot_damage_taken
  from `bdpproject-eve.wars.t_loser_aggregate`
  group by 1,2
  ) b
on a.winner_id = b.loser_id
);

-- Create aggregate table for win-loss (Edges) Old
create table wars.battle_outcomes as
select a.winner_id as src, b.loser_id as dst, a.damage_done as damage_done
from `bdpproject-eve.wars.t_winner_aggregate` a,
`bdpproject-eve.wars.t_loser_aggregate` b
where a.killmail_id = b.killmail_id;
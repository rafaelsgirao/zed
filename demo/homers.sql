SELECT people.nameFirst, people.nameLast, batting.HR, teams.name
FROM '{playerID:string,yearID:float64,stint:float64,teamID:string,lgID:string,G:float64,AB:float64,R:float64,H:float64,"2B":float64,"3B":float64,HR:float64,RBI:float64,SB:float64,CS:float64,BB:float64,SO:float64,IBB:float64,HBP:float64,SH:float64,SF:float64,GIDP:float64}' batting
  JOIN '{playerID:string,birthYear:float64,birthMonth:float64,birthDay:float64,birthCountry:string,birthState:string,birthCity:string,deathYear:float64,deathMonth:float64,deathDay:float64,deathCountry:string,deathState:string,deathCity:string,nameFirst:string,nameLast:string,nameGiven:string,weight:float64,height:float64,bats:string,throws:string,debut:string,finalGame:string,retroID:string,bbrefID:string}' people
    ON batting.playerID = people.playerID
  JOIN '{yearID:float64,lgID:string,teamID:string,franchID:string,divID:string,Rank:float64,G:float64,Ghome:float64,W:float64,L:float64,DivWin:string,WCWin:string,LgWin:string,WSWin:string,R:float64,AB:float64,H:float64,"2B":float64,"3B":float64,HR:float64,BB:float64,SO:float64,SB:float64,CS:float64,HBP:float64,SF:float64,RA:float64,ER:float64,ERA:float64,CG:float64,SHO:float64,SV:float64,IPouts:float64,HA:float64,HRA:float64,BBA:float64,SOA:float64,E:float64,DP:float64,FP:float64,name:string,park:string,attendance:float64,BPF:float64,PPF:float64,teamIDBR:string,teamIDlahman45:string,teamIDretro:string}' teams
    ON batting.teamID = teams.teamID
WHERE batting.yearID = 1977 AND teams.yearID = 1977
ORDER BY batting.HR DESC
LIMIT 20

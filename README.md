
<b> Projet DataScience :</b> DataCrime

<b> Groupe de travail : </b>
- <b>Yann SIMON</b> (Chef de Projet)
- Romain DUCROS
- Alexandre QUENTIN
- Rémi SAULNERON


<b>Install :</b>

 setup :

 Install python

 <b> install python mysql-connector </b>
> pip install mysql-connector

 <b> install plotly </b>
> pip install plotly
> pip install plotly --upgrade
> pip install scipy
> pip install numpy
> pip install python-dotenv
> pip install astral



create folder where you want install greenplace
open cmd
git clone greenplace repositories
> git clone https://github.com/ProDiG31/DataScience_DataCrime.git


<b> TO do :</b>

    - extract Status type
    - import crime type gravity
    - map chart


<b>Analysis Ideas :</b>

 - [ ] Crimes / Plein Lune
 - [ ] Crimes / Taux de chomages
 - [ ] Crimes / Famille MonoParentales
 - [ ] Crimes / Ethnie
 - [ ] Crimes / Saison

heath map (axeY=année, axeX=catégorie/gravité, valeur= nombre de crime)

<b>Analysis BRUTE :</b>

A faire sur Année / Saison (Trimestres)
Arme la plus utilisé de l'année
Victime la plus probable
Lieu le plus dangereux
Rape, Robbery, murder, Assault
Nombre de delit par ans
% crime par category

crime by category :
    Personnal crime
    Property Crime
    Inchoate Crime (crime incomplet)
    Statutory crime

Top 5 crime_code / année

<b>Sources:</b>
Los Angeles Population
>https://fred.stlouisfed.org/series/CALOSA7POP

Unemployement_rate_LA.csv
>https://fred.stlouisfed.org/series/CALOSA7URN

SingleParentRate.csv
>https://fred.stlouisfed.org/series/S1101SPHOUSE006037
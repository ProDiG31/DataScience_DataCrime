// no available on ie
// Alexandre QUENTIN @Leidanium
// for https://www.timeanddate.com/moon/phases/usa/los-angeles?year=2018
// at 13/10/2018
// Full JS

function treat_date(siteDate) {
    [day, month] = siteDate.split(' ');
    day = add_digit(day);
    month = month_hash[month];
    fullMoonDates.push(month + '/' + day +'/'+year);
}

function add_digit(number) {
    if (number.length == 1) {
        number = '0'+ number;
    }
    return number;
}

var table = document.getElementsByTagName("tbody")[1].children;

var month_hash = { 'jan':'01', 'fév':'02', 'mar':'03', 'avr':'04', 'mai':'05', 'juin':'06', 
                   'juil':'07', 'aoû':'08', 'sep':'09', 'oct':'10', 'nov':'11', 'déc':'12'
                 };

var month, day;

// MM/JJ/AAAA
year = document.querySelector("body > div.wrapper > div.main-content-div > section.fixed > div.row.dashb.pdflexi > form > div > input[type='text']").value;
var fullMoonDates = [];

for (let i = 0; i < table.length; i++) {
    treat_date(table[i].children[5].innerHTML);
}
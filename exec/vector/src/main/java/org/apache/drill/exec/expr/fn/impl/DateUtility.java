/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.expr.fn.impl;

import org.joda.time.Period;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.DateTimeParser;

import com.carrotsearch.hppc.ObjectIntHashMap;

// Utility class for Date, DateTime, TimeStamp, Interval data types
public class DateUtility {


    /* We have a hashmap that stores the timezone as the key and an index as the value
     * While storing the timezone in value vectors, holders we only use this index. As we
     * reconstruct the timestamp, we use this index to index through the array timezoneList
     * and get the corresponding timezone and pass it to joda-time
     */
  public static ObjectIntHashMap<String> timezoneMap = new ObjectIntHashMap<String>();

    public static String[] timezoneList =  {"Africa/Abidjan",
                                            "Africa/Accra",
                                            "Africa/Addis_Ababa",
                                            "Africa/Algiers",
                                            "Africa/Asmara",
                                            "Africa/Asmera",
                                            "Africa/Bamako",
                                            "Africa/Bangui",
                                            "Africa/Banjul",
                                            "Africa/Bissau",
                                            "Africa/Blantyre",
                                            "Africa/Brazzaville",
                                            "Africa/Bujumbura",
                                            "Africa/Cairo",
                                            "Africa/Casablanca",
                                            "Africa/Ceuta",
                                            "Africa/Conakry",
                                            "Africa/Dakar",
                                            "Africa/Dar_es_Salaam",
                                            "Africa/Djibouti",
                                            "Africa/Douala",
                                            "Africa/El_Aaiun",
                                            "Africa/Freetown",
                                            "Africa/Gaborone",
                                            "Africa/Harare",
                                            "Africa/Johannesburg",
                                            "Africa/Juba",
                                            "Africa/Kampala",
                                            "Africa/Khartoum",
                                            "Africa/Kigali",
                                            "Africa/Kinshasa",
                                            "Africa/Lagos",
                                            "Africa/Libreville",
                                            "Africa/Lome",
                                            "Africa/Luanda",
                                            "Africa/Lubumbashi",
                                            "Africa/Lusaka",
                                            "Africa/Malabo",
                                            "Africa/Maputo",
                                            "Africa/Maseru",
                                            "Africa/Mbabane",
                                            "Africa/Mogadishu",
                                            "Africa/Monrovia",
                                            "Africa/Nairobi",
                                            "Africa/Ndjamena",
                                            "Africa/Niamey",
                                            "Africa/Nouakchott",
                                            "Africa/Ouagadougou",
                                            "Africa/Porto-Novo",
                                            "Africa/Sao_Tome",
                                            "Africa/Timbuktu",
                                            "Africa/Tripoli",
                                            "Africa/Tunis",
                                            "Africa/Windhoek",
                                            "America/Adak",
                                            "America/Anchorage",
                                            "America/Anguilla",
                                            "America/Antigua",
                                            "America/Araguaina",
                                            "America/Argentina/Buenos_Aires",
                                            "America/Argentina/Catamarca",
                                            "America/Argentina/ComodRivadavia",
                                            "America/Argentina/Cordoba",
                                            "America/Argentina/Jujuy",
                                            "America/Argentina/La_Rioja",
                                            "America/Argentina/Mendoza",
                                            "America/Argentina/Rio_Gallegos",
                                            "America/Argentina/Salta",
                                            "America/Argentina/San_Juan",
                                            "America/Argentina/San_Luis",
                                            "America/Argentina/Tucuman",
                                            "America/Argentina/Ushuaia",
                                            "America/Aruba",
                                            "America/Asuncion",
                                            "America/Atikokan",
                                            "America/Atka",
                                            "America/Bahia",
                                            "America/Bahia_Banderas",
                                            "America/Barbados",
                                            "America/Belem",
                                            "America/Belize",
                                            "America/Blanc-Sablon",
                                            "America/Boa_Vista",
                                            "America/Bogota",
                                            "America/Boise",
                                            "America/Buenos_Aires",
                                            "America/Cambridge_Bay",
                                            "America/Campo_Grande",
                                            "America/Cancun",
                                            "America/Caracas",
                                            "America/Catamarca",
                                            "America/Cayenne",
                                            "America/Cayman",
                                            "America/Chicago",
                                            "America/Chihuahua",
                                            "America/Coral_Harbour",
                                            "America/Cordoba",
                                            "America/Costa_Rica",
                                            "America/Cuiaba",
                                            "America/Curacao",
                                            "America/Danmarkshavn",
                                            "America/Dawson",
                                            "America/Dawson_Creek",
                                            "America/Denver",
                                            "America/Detroit",
                                            "America/Dominica",
                                            "America/Edmonton",
                                            "America/Eirunepe",
                                            "America/El_Salvador",
                                            "America/Ensenada",
                                            "America/Fort_Wayne",
                                            "America/Fortaleza",
                                            "America/Glace_Bay",
                                            "America/Godthab",
                                            "America/Goose_Bay",
                                            "America/Grand_Turk",
                                            "America/Grenada",
                                            "America/Guadeloupe",
                                            "America/Guatemala",
                                            "America/Guayaquil",
                                            "America/Guyana",
                                            "America/Halifax",
                                            "America/Havana",
                                            "America/Hermosillo",
                                            "America/Indiana/Indianapolis",
                                            "America/Indiana/Knox",
                                            "America/Indiana/Marengo",
                                            "America/Indiana/Petersburg",
                                            "America/Indiana/Tell_City",
                                            "America/Indiana/Vevay",
                                            "America/Indiana/Vincennes",
                                            "America/Indiana/Winamac",
                                            "America/Indianapolis",
                                            "America/Inuvik",
                                            "America/Iqaluit",
                                            "America/Jamaica",
                                            "America/Jujuy",
                                            "America/Juneau",
                                            "America/Kentucky/Louisville",
                                            "America/Kentucky/Monticello",
                                            "America/Knox_IN",
                                            "America/Kralendijk",
                                            "America/La_Paz",
                                            "America/Lima",
                                            "America/Los_Angeles",
                                            "America/Louisville",
                                            "America/Lower_Princes",
                                            "America/Maceio",
                                            "America/Managua",
                                            "America/Manaus",
                                            "America/Marigot",
                                            "America/Martinique",
                                            "America/Matamoros",
                                            "America/Mazatlan",
                                            "America/Mendoza",
                                            "America/Menominee",
                                            "America/Merida",
                                            "America/Metlakatla",
                                            "America/Mexico_City",
                                            "America/Miquelon",
                                            "America/Moncton",
                                            "America/Monterrey",
                                            "America/Montevideo",
                                            "America/Montreal",
                                            "America/Montserrat",
                                            "America/Nassau",
                                            "America/New_York",
                                            "America/Nipigon",
                                            "America/Nome",
                                            "America/Noronha",
                                            "America/North_Dakota/Beulah",
                                            "America/North_Dakota/Center",
                                            "America/North_Dakota/New_Salem",
                                            "America/Ojinaga",
                                            "America/Panama",
                                            "America/Pangnirtung",
                                            "America/Paramaribo",
                                            "America/Phoenix",
                                            "America/Port-au-Prince",
                                            "America/Port_of_Spain",
                                            "America/Porto_Acre",
                                            "America/Porto_Velho",
                                            "America/Puerto_Rico",
                                            "America/Rainy_River",
                                            "America/Rankin_Inlet",
                                            "America/Recife",
                                            "America/Regina",
                                            "America/Resolute",
                                            "America/Rio_Branco",
                                            "America/Rosario",
                                            "America/Santa_Isabel",
                                            "America/Santarem",
                                            "America/Santiago",
                                            "America/Santo_Domingo",
                                            "America/Sao_Paulo",
                                            "America/Scoresbysund",
                                            "America/Shiprock",
                                            "America/Sitka",
                                            "America/St_Barthelemy",
                                            "America/St_Johns",
                                            "America/St_Kitts",
                                            "America/St_Lucia",
                                            "America/St_Thomas",
                                            "America/St_Vincent",
                                            "America/Swift_Current",
                                            "America/Tegucigalpa",
                                            "America/Thule",
                                            "America/Thunder_Bay",
                                            "America/Tijuana",
                                            "America/Toronto",
                                            "America/Tortola",
                                            "America/Vancouver",
                                            "America/Virgin",
                                            "America/Whitehorse",
                                            "America/Winnipeg",
                                            "America/Yakutat",
                                            "America/Yellowknife",
                                            "Antarctica/Casey",
                                            "Antarctica/Davis",
                                            "Antarctica/DumontDUrville",
                                            "Antarctica/Macquarie",
                                            "Antarctica/Mawson",
                                            "Antarctica/McMurdo",
                                            "Antarctica/Palmer",
                                            "Antarctica/Rothera",
                                            "Antarctica/South_Pole",
                                            "Antarctica/Syowa",
                                            "Antarctica/Vostok",
                                            "Arctic/Longyearbyen",
                                            "Asia/Aden",
                                            "Asia/Almaty",
                                            "Asia/Amman",
                                            "Asia/Anadyr",
                                            "Asia/Aqtau",
                                            "Asia/Aqtobe",
                                            "Asia/Ashgabat",
                                            "Asia/Ashkhabad",
                                            "Asia/Baghdad",
                                            "Asia/Bahrain",
                                            "Asia/Baku",
                                            "Asia/Bangkok",
                                            "Asia/Beirut",
                                            "Asia/Bishkek",
                                            "Asia/Brunei",
                                            "Asia/Calcutta",
                                            "Asia/Choibalsan",
                                            "Asia/Chongqing",
                                            "Asia/Chungking",
                                            "Asia/Colombo",
                                            "Asia/Dacca",
                                            "Asia/Damascus",
                                            "Asia/Dhaka",
                                            "Asia/Dili",
                                            "Asia/Dubai",
                                            "Asia/Dushanbe",
                                            "Asia/Gaza",
                                            "Asia/Harbin",
                                            "Asia/Hebron",
                                            "Asia/Ho_Chi_Minh",
                                            "Asia/Hong_Kong",
                                            "Asia/Hovd",
                                            "Asia/Irkutsk",
                                            "Asia/Istanbul",
                                            "Asia/Jakarta",
                                            "Asia/Jayapura",
                                            "Asia/Jerusalem",
                                            "Asia/Kabul",
                                            "Asia/Kamchatka",
                                            "Asia/Karachi",
                                            "Asia/Kashgar",
                                            "Asia/Kathmandu",
                                            "Asia/Katmandu",
                                            "Asia/Kolkata",
                                            "Asia/Krasnoyarsk",
                                            "Asia/Kuala_Lumpur",
                                            "Asia/Kuching",
                                            "Asia/Kuwait",
                                            "Asia/Macao",
                                            "Asia/Macau",
                                            "Asia/Magadan",
                                            "Asia/Makassar",
                                            "Asia/Manila",
                                            "Asia/Muscat",
                                            "Asia/Nicosia",
                                            "Asia/Novokuznetsk",
                                            "Asia/Novosibirsk",
                                            "Asia/Omsk",
                                            "Asia/Oral",
                                            "Asia/Phnom_Penh",
                                            "Asia/Pontianak",
                                            "Asia/Pyongyang",
                                            "Asia/Qatar",
                                            "Asia/Qyzylorda",
                                            "Asia/Rangoon",
                                            "Asia/Riyadh",
                                            "Asia/Saigon",
                                            "Asia/Sakhalin",
                                            "Asia/Samarkand",
                                            "Asia/Seoul",
                                            "Asia/Shanghai",
                                            "Asia/Singapore",
                                            "Asia/Taipei",
                                            "Asia/Tashkent",
                                            "Asia/Tbilisi",
                                            "Asia/Tehran",
                                            "Asia/Tel_Aviv",
                                            "Asia/Thimbu",
                                            "Asia/Thimphu",
                                            "Asia/Tokyo",
                                            "Asia/Ujung_Pandang",
                                            "Asia/Ulaanbaatar",
                                            "Asia/Ulan_Bator",
                                            "Asia/Urumqi",
                                            "Asia/Vientiane",
                                            "Asia/Vladivostok",
                                            "Asia/Yakutsk",
                                            "Asia/Yekaterinburg",
                                            "Asia/Yerevan",
                                            "Atlantic/Azores",
                                            "Atlantic/Bermuda",
                                            "Atlantic/Canary",
                                            "Atlantic/Cape_Verde",
                                            "Atlantic/Faeroe",
                                            "Atlantic/Faroe",
                                            "Atlantic/Jan_Mayen",
                                            "Atlantic/Madeira",
                                            "Atlantic/Reykjavik",
                                            "Atlantic/South_Georgia",
                                            "Atlantic/St_Helena",
                                            "Atlantic/Stanley",
                                            "Australia/ACT",
                                            "Australia/Adelaide",
                                            "Australia/Brisbane",
                                            "Australia/Broken_Hill",
                                            "Australia/Canberra",
                                            "Australia/Currie",
                                            "Australia/Darwin",
                                            "Australia/Eucla",
                                            "Australia/Hobart",
                                            "Australia/LHI",
                                            "Australia/Lindeman",
                                            "Australia/Lord_Howe",
                                            "Australia/Melbourne",
                                            "Australia/NSW",
                                            "Australia/North",
                                            "Australia/Perth",
                                            "Australia/Queensland",
                                            "Australia/South",
                                            "Australia/Sydney",
                                            "Australia/Tasmania",
                                            "Australia/Victoria",
                                            "Australia/West",
                                            "Australia/Yancowinna",
                                            "Brazil/Acre",
                                            "Brazil/DeNoronha",
                                            "Brazil/East",
                                            "Brazil/West",
                                            "CET",
                                            "CST6CDT",
                                            "Canada/Atlantic",
                                            "Canada/Central",
                                            "Canada/East-Saskatchewan",
                                            "Canada/Eastern",
                                            "Canada/Mountain",
                                            "Canada/Newfoundland",
                                            "Canada/Pacific",
                                            "Canada/Saskatchewan",
                                            "Canada/Yukon",
                                            "Chile/Continental",
                                            "Chile/EasterIsland",
                                            "Cuba",
                                            "EET",
                                            "EST",
                                            "EST5EDT",
                                            "Egypt",
                                            "Eire",
                                            "Etc/GMT",
                                            "Etc/GMT+0",
                                            "Etc/GMT+1",
                                            "Etc/GMT+10",
                                            "Etc/GMT+11",
                                            "Etc/GMT+12",
                                            "Etc/GMT+2",
                                            "Etc/GMT+3",
                                            "Etc/GMT+4",
                                            "Etc/GMT+5",
                                            "Etc/GMT+6",
                                            "Etc/GMT+7",
                                            "Etc/GMT+8",
                                            "Etc/GMT+9",
                                            "Etc/GMT-0",
                                            "Etc/GMT-1",
                                            "Etc/GMT-10",
                                            "Etc/GMT-11",
                                            "Etc/GMT-12",
                                            "Etc/GMT-13",
                                            "Etc/GMT-14",
                                            "Etc/GMT-2",
                                            "Etc/GMT-3",
                                            "Etc/GMT-4",
                                            "Etc/GMT-5",
                                            "Etc/GMT-6",
                                            "Etc/GMT-7",
                                            "Etc/GMT-8",
                                            "Etc/GMT-9",
                                            "Etc/GMT0",
                                            "Etc/Greenwich",
                                            "Etc/UCT",
                                            "Etc/UTC",
                                            "Etc/Universal",
                                            "Etc/Zulu",
                                            "Europe/Amsterdam",
                                            "Europe/Andorra",
                                            "Europe/Athens",
                                            "Europe/Belfast",
                                            "Europe/Belgrade",
                                            "Europe/Berlin",
                                            "Europe/Bratislava",
                                            "Europe/Brussels",
                                            "Europe/Bucharest",
                                            "Europe/Budapest",
                                            "Europe/Chisinau",
                                            "Europe/Copenhagen",
                                            "Europe/Dublin",
                                            "Europe/Gibraltar",
                                            "Europe/Guernsey",
                                            "Europe/Helsinki",
                                            "Europe/Isle_of_Man",
                                            "Europe/Istanbul",
                                            "Europe/Jersey",
                                            "Europe/Kaliningrad",
                                            "Europe/Kiev",
                                            "Europe/Lisbon",
                                            "Europe/Ljubljana",
                                            "Europe/London",
                                            "Europe/Luxembourg",
                                            "Europe/Madrid",
                                            "Europe/Malta",
                                            "Europe/Mariehamn",
                                            "Europe/Minsk",
                                            "Europe/Monaco",
                                            "Europe/Moscow",
                                            "Europe/Nicosia",
                                            "Europe/Oslo",
                                            "Europe/Paris",
                                            "Europe/Podgorica",
                                            "Europe/Prague",
                                            "Europe/Riga",
                                            "Europe/Rome",
                                            "Europe/Samara",
                                            "Europe/San_Marino",
                                            "Europe/Sarajevo",
                                            "Europe/Simferopol",
                                            "Europe/Skopje",
                                            "Europe/Sofia",
                                            "Europe/Stockholm",
                                            "Europe/Tallinn",
                                            "Europe/Tirane",
                                            "Europe/Tiraspol",
                                            "Europe/Uzhgorod",
                                            "Europe/Vaduz",
                                            "Europe/Vatican",
                                            "Europe/Vienna",
                                            "Europe/Vilnius",
                                            "Europe/Volgograd",
                                            "Europe/Warsaw",
                                            "Europe/Zagreb",
                                            "Europe/Zaporozhye",
                                            "Europe/Zurich",
                                            "GB",
                                            "GB-Eire",
                                            "GMT",
                                            "GMT+0",
                                            "GMT-0",
                                            "GMT0",
                                            "Greenwich",
                                            "HST",
                                            "Hongkong",
                                            "Iceland",
                                            "Indian/Antananarivo",
                                            "Indian/Chagos",
                                            "Indian/Christmas",
                                            "Indian/Cocos",
                                            "Indian/Comoro",
                                            "Indian/Kerguelen",
                                            "Indian/Mahe",
                                            "Indian/Maldives",
                                            "Indian/Mauritius",
                                            "Indian/Mayotte",
                                            "Indian/Reunion",
                                            "Iran",
                                            "Israel",
                                            "Jamaica",
                                            "Japan",
                                            "Kwajalein",
                                            "Libya",
                                            "MET",
                                            "MST",
                                            "MST7MDT",
                                            "Mexico/BajaNorte",
                                            "Mexico/BajaSur",
                                            "Mexico/General",
                                            "NZ",
                                            "NZ-CHAT",
                                            "Navajo",
                                            "PRC",
                                            "PST8PDT",
                                            "Pacific/Apia",
                                            "Pacific/Auckland",
                                            "Pacific/Chatham",
                                            "Pacific/Chuuk",
                                            "Pacific/Easter",
                                            "Pacific/Efate",
                                            "Pacific/Enderbury",
                                            "Pacific/Fakaofo",
                                            "Pacific/Fiji",
                                            "Pacific/Funafuti",
                                            "Pacific/Galapagos",
                                            "Pacific/Gambier",
                                            "Pacific/Guadalcanal",
                                            "Pacific/Guam",
                                            "Pacific/Honolulu",
                                            "Pacific/Johnston",
                                            "Pacific/Kiritimati",
                                            "Pacific/Kosrae",
                                            "Pacific/Kwajalein",
                                            "Pacific/Majuro",
                                            "Pacific/Marquesas",
                                            "Pacific/Midway",
                                            "Pacific/Nauru",
                                            "Pacific/Niue",
                                            "Pacific/Norfolk",
                                            "Pacific/Noumea",
                                            "Pacific/Pago_Pago",
                                            "Pacific/Palau",
                                            "Pacific/Pitcairn",
                                            "Pacific/Pohnpei",
                                            "Pacific/Ponape",
                                            "Pacific/Port_Moresby",
                                            "Pacific/Rarotonga",
                                            "Pacific/Saipan",
                                            "Pacific/Samoa",
                                            "Pacific/Tahiti",
                                            "Pacific/Tarawa",
                                            "Pacific/Tongatapu",
                                            "Pacific/Truk",
                                            "Pacific/Wake",
                                            "Pacific/Wallis",
                                            "Pacific/Yap",
                                            "Poland",
                                            "Portugal",
                                            "ROC",
                                            "ROK",
                                            "Singapore",
                                            "Turkey",
                                            "UCT",
                                            "US/Alaska",
                                            "US/Aleutian",
                                            "US/Arizona",
                                            "US/Central",
                                            "US/East-Indiana",
                                            "US/Eastern",
                                            "US/Hawaii",
                                            "US/Indiana-Starke",
                                            "US/Michigan",
                                            "US/Mountain",
                                            "US/Pacific",
                                            "US/Pacific-New",
                                            "US/Samoa",
                                            "UTC",
                                            "Universal",
                                            "W-SU",
                                            "WET",
                                            "Zulu"};

    static {
      for (int i = 0; i < timezoneList.length; i++) {
        timezoneMap.put(timezoneList[i], i);
      }
    }

    public static final DateTimeFormatter formatDate        = DateTimeFormat.forPattern("yyyy-MM-dd");
    public static final DateTimeFormatter formatTimeStamp    = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
    public static final DateTimeFormatter formatTimeStampTZ = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS ZZZ");
    public static final DateTimeFormatter formatTime        = DateTimeFormat.forPattern("HH:mm:ss.SSS");

    public static DateTimeFormatter dateTimeTZFormat = null;
    public static DateTimeFormatter timeFormat = null;

    public static final int yearsToMonths = 12;
    public static final int hoursToMillis = 60 * 60 * 1000;
    public static final int minutesToMillis = 60 * 1000;
    public static final int secondsToMillis = 1000;
    public static final int monthToStandardDays = 30;
    public static final long monthsToMillis = 2592000000L; // 30 * 24 * 60 * 60 * 1000
    public static final int daysToStandardMillis = 24 * 60 * 60 * 1000;


  public static int getIndex(String timezone) {
    return timezoneMap.get(timezone);
  }

  public static String getTimeZone(int index) {
    return timezoneList[index];
  }

  // Returns the date time formatter used to parse date strings
  public static DateTimeFormatter getDateTimeFormatter() {

    if (dateTimeTZFormat == null) {
      DateTimeFormatter dateFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");
      DateTimeParser optionalTime = DateTimeFormat.forPattern(" HH:mm:ss").getParser();
      DateTimeParser optionalSec = DateTimeFormat.forPattern(".SSS").getParser();
      DateTimeParser optionalZone = DateTimeFormat.forPattern(" ZZZ").getParser();

      dateTimeTZFormat = new DateTimeFormatterBuilder().append(dateFormatter).appendOptional(optionalTime).appendOptional(optionalSec).appendOptional(optionalZone).toFormatter();
    }

    return dateTimeTZFormat;
  }

  // Returns time formatter used to parse time strings
  public static DateTimeFormatter getTimeFormatter() {
    if (timeFormat == null) {
      DateTimeFormatter timeFormatter = DateTimeFormat.forPattern("HH:mm:ss");
      DateTimeParser optionalSec = DateTimeFormat.forPattern(".SSS").getParser();
      timeFormat = new DateTimeFormatterBuilder().append(timeFormatter).appendOptional(optionalSec).toFormatter();
    }
    return timeFormat;
  }

  public static int monthsFromPeriod(Period period){
    return (period.getYears() * yearsToMonths) + period.getMonths();
  }

  public static int periodToMillis(final Period period){
    return (period.getHours() * hoursToMillis) +
           (period.getMinutes() * minutesToMillis) +
           (period.getSeconds() * secondsToMillis) +
           (period.getMillis());
  }

  public static int toMonths(int years, int months) {
    return years * yearsToMonths + months;
  }

  public static int periodToMonths(Period value) {
    return value.getYears() * yearsToMonths + value.getMonths();
  }

  public static Period fromIntervalYear(int value) {
    final int years  = (value / yearsToMonths);
    final int months = (value % yearsToMonths);
    return new Period()
        .plusYears(years)
        .plusMonths(months);
  }

  public static StringBuilder intervalYearStringBuilder(int months) {
    final int years = months / yearsToMonths;
    months %= yearsToMonths;

    return new StringBuilder()
           .append(years)
           .append(pluralify("year", years))
           .append(" ")
           .append(months)
           .append(pluralify("month", months));
  }

  public static StringBuilder intervalYearStringBuilder(Period value) {
    return intervalYearStringBuilder(
        value.getYears() * 12 + value.getMonths());
  }

  public static String pluralify(String term, int value) {
    term = (Math.abs(value) == 1) ? term : term + "s";
    return " " + term;
  }

  public static Period fromIntervalDay(int days, int millis) {
    return new Period()
        .plusDays(days)
        .plusMillis(millis);
  }

  public static StringBuilder intervalDayStringBuilder(int days, int millis) {

    final int hours  = millis / (DateUtility.hoursToMillis);
    millis %= (DateUtility.hoursToMillis);

    final int minutes = millis / (DateUtility.minutesToMillis);
    millis %= (DateUtility.minutesToMillis);

    final int seconds = millis / (DateUtility.secondsToMillis);
    millis %= (DateUtility.secondsToMillis);

    StringBuilder buf = new StringBuilder()
            .append(days)
            .append(pluralify("day", days))
            .append(" ")
            .append(hours)
            .append(":")
            .append(asTwoDigits(minutes))
            .append(":")
            .append(asTwoDigits(seconds));
    if (millis != 0) {
      buf.append(".")
         .append(millis);
    }
    return buf;
  }

  public static StringBuilder intervalDayStringBuilder(Period value) {
    return intervalDayStringBuilder(
        value.getDays(),
        periodToMillis(value));
  }

  public static Period fromInterval(int months, int days, int millis) {
    return new Period()
        .plusMonths(months)
        .plusDays(days)
        .plusMillis(millis);
  }

  public static String asTwoDigits(int value) {
    return String.format("%02d", value);
  }

  public static StringBuilder intervalStringBuilder(int months, int days, int millis) {

    final int years = months / yearsToMonths;
    months %= yearsToMonths;

    final int hours  = millis / hoursToMillis;
    millis %= hoursToMillis;

    final int minutes = millis / minutesToMillis;
    millis %= minutesToMillis;

    final int seconds = millis / secondsToMillis;
    millis %= secondsToMillis;

    StringBuilder buf = new StringBuilder()
           .append(years)
           .append(pluralify("year", years))
           .append(" ")
           .append(months)
           .append(pluralify("month", months))
           .append(" ")
           .append(days)
           .append(pluralify("day", days))
           .append(" ")
           .append(hours)
           .append(":")
           .append(asTwoDigits(minutes))
           .append(":")
           .append(asTwoDigits(seconds));
    if (millis != 0) {
      buf.append(".")
         .append(millis);
    }
    return buf;
  }

  public static StringBuilder intervalStringBuilder(Period value) {
    return intervalStringBuilder(
        value.getYears() * 12 + value.getMonths(),
        value.getDays(),
        periodToMillis(value));
  }

  public static int timeToMillis(int hours, int minutes, int seconds, int millis) {
    return ((hours * 60 +
             minutes) * 60 +
            seconds) * 1000 +
           millis;
  }
}

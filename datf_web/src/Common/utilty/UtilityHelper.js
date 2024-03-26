/*function to get count of Live status */
import { differenceInDays, format } from "date-fns";
import _ from "lodash";


export function formatCurrency(amt, inThhousends) {
    if (!amt) return '-'

    var formatter = new Intl.NumberFormat('en-US', {
        style: 'currency',
        currency: 'INR',
        minimumFractionDigits: 0
    });

    if (inThhousends && amt > 1000)
        return `${formatter.format(Math.round(amt / 1000))}K`

    return formatter.format(amt)
    
}
export function convertDate(inputFormat) {
    function pad(s) { return (s < 10) ? '0' + s : s; }
    var d = new Date(inputFormat)
    return [pad(d.getFullYear()), pad(d.getMonth()+1),pad(d.getDate()) ].join('-')
  }

export function formatNumber(n, inThhousends = false) {
    if (isNaN(n)) return '-'
    var formatter = new Intl.NumberFormat('en-US');
    if (inThhousends)
        return `${formatter.format(n / 1000)}K`

    return formatter.format(n)
}

export function formatDate(date, dateFormat = 'dd MMM, yyyy HH:mm') {
    if (!date) return "-";

    try {
        return format(parseDate(date), dateFormat);
    } catch (e) { console.log(e) }

    return '-'
}

export function parseDate(date) {
    let newDate = date;

    if (newDate instanceof Date) {
        return date
    }

    /* if (newDate.includes('.000+00:00')) {
        newDate = newDate.replace('.000+00:00', '')
    } */

    return new Date(newDate)
}

export function lastdate(days) {
    let date = new Date();
    return date.setDate(date.getDate() - days);
}

export function calcPercentage(partialValue, totalValue) {
    const result = (100 * partialValue) / totalValue;
    return result.toFixed(2);
}

export function calcRegionData(data) {
    let total = 0;
    let obj = {};
    if (data) {
        data.forEach((item) => {
            total += item.total;
        });
        data.forEach((item) => {
            var itemtotal = item.total;
            obj[item._id] = { "value": itemtotal, "per": calcPercentage(itemtotal, total) + " % " };
        });
    }
    return obj;
}

export function formatAMPM(date) {
    var hours = date.getHours();
    var minutes = date.getMinutes();
    var ampm = hours >= 12 ? 'pm' : 'am';
    hours = hours % 12;
    hours = hours ? hours : 12; // the hour '0' should be '12'
    minutes = minutes < 10 ? '0' + minutes : minutes;
    var strTime = hours + ':' + minutes + ' ' + ampm;
    return strTime;
}

export function parseDateAndTime(dateTime, dateWithYear = false) {
    const monthNames = ["Jan", "Feb", "March", "April", "May", "June",
        "July", "Aug", "Sep", "Oct", "Nov", "Dec"
    ];
    const modifiedDateTime = new Date(dateTime);
    if (dateWithYear) {
        return modifiedDateTime.getDate() + " " + monthNames[modifiedDateTime.getMonth()] + " " + modifiedDateTime.getFullYear();
    }

    return modifiedDateTime.getDate() + " " + monthNames[modifiedDateTime.getMonth()] + ", " + formatAMPM(modifiedDateTime);
}

export function parseTime(dateTime) {
    const modifiedDateTime = new Date(dateTime);
    return formatAMPM(modifiedDateTime);
}
export function sortByTimestamp(array, key) {
    let newarray = array.slice().sort((a, b) => new Date(b[key]) - new Date(a[key]));
    return newarray;
}

export function findInObjArr(arr, arrVal, findKey) {
    let result = arr.find((o, i) => o[findKey] === arrVal);
    return result;
}
export function removeInObjArr(arr, attr, value) {
    var i = arr.length;
    while (i--) {
        if (arr[i]
            && arr[i].hasOwnProperty(attr)
            && (arguments.length > 2 && arr[i][attr] === value)) {

            arr.splice(i, 1);

        }
    }
    return arr;
}
export function updateInObjArr(arr, attr, value, updatedValue) {
    var i = arr.length;
    const arrCopy = [...arr];
    while (i--) {
        if (arr[i]
            && arr[i].hasOwnProperty(attr)
            && (arguments.length > 3 && arr[i][attr] === value)) {

            arrCopy[i] = updatedValue;

        }
    }
    return arrCopy;
}

export function onlyUnique(value, index, self) {
    return self.indexOf(value) === index;
}


export function filterByValue(arr, arrVal, findKey) {
    let result = arr.filter((o, i) => o[findKey] === arrVal);
    return result;
}

export function filterByProp(arr, col, val) {
    const results = arr.filter(obj => {
        return obj[col] === val;
    });
    return results;
}
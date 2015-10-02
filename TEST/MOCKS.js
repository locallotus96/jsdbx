var db = require('../LIB/JSDBX.js');

console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
var cName = 'test';
db.connect('DB/TEST', cName, function(err) {
    if(err) {
        console.log(err);
    } else {
        test();
    }
});

function test() {
    insertTestRecords(100000, cName);
    console.time('Find Time');
    var list = db.test.find();
    console.timeEnd('Find Time');
    console.log(list.length + ' records');

    console.time('Reverse (for push then splice) Time');
    UTIL.reverseList(list); // very slow
    console.timeEnd('Reverse (for push then splice) Time');

    console.time('Reverse (native method) Time');
    list.reverse(); // almost instant
    console.timeEnd('Reverse (native method) Time');

    console.time('Copy (while loop) Time');
    var a = UTIL.copyList(list); // very good
    console.timeEnd('Copy (while loop) Time');

    console.time('Copy (slice) Time');
    var b = list.slice(); // almost instant
    console.timeEnd('Copy (slice) Time');

    var sort = {iq:-1};
    console.time('Quicksort Time');
    UTIL.quickSort(a, sort);
    console.timeEnd('Quicksort Time');

    console.time('Selection-sort Time');
    UTIL.selectionSort(b, Object.keys(sort)[0]);
    console.timeEnd('Selection-sort Time');
}

function insertTestRecords(x, cName) {
    console.log('|||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||')
    console.time(':: Insert Time');
    for (var i = 0; i < x; i++) {
        db[cName].insert({
            name: 'Tim' + i,
            surname: 'Richards' + i,
            //password: crypto.randomBytes(8),
            score: i, // index
            teacher: 'Bob',
            city: 'Cape Town',
            age: Math.floor((Math.random() * 100) + 1),
            iq: Math.floor((Math.random() * 2000) - 1000),
            obj: {school:'Highschool', nqflevel:(Math.ceil(Math.random() * 9)+1)}
        });
    };
    console.timeEnd(':: Insert Time');
    console.log('<DB> Records:', db[cName].count());
}

var UTIL = {};

/*
    Swap two elements of an array in place
    Used in quick-sort implementation in UTIL.partition()
    Used in selection-sort implementation in UTIL.selectionSort()
*/
UTIL.swap = function (collection, iOne, iTwo) {
    var temp = collection[iOne];
    collection[iOne] = collection[iTwo];
    collection[iTwo] = temp;
}

/*
    Used in quicksort implementation in UTIL.quickSort()
    Binary partition
*/
UTIL.partition = function (collection, field, left, right) {
    //console.log(field, left, right);
    /*
        Selecting a pivot (middle) element is also complicated by the existence of integer overflow.
        If the boundary indices of the subarray being sorted are sufficiently large,
        the naïve expression for the middle index, (lo + hi)/2, will cause overflow and provide an invalid pivot index.
        This can be overcome by using, for example, lo + (hi−lo)/2 to index the middle element, at the cost of more complex arithmetic.
    */
    //var pivot = collection[Math.floor((right + left) / 2)]; // middle index
    var pivot = collection[Math.floor(left + (right - left) / 2)]; // middle object
    var i = left; // starts from left and goes to pivot index
    var j = right; // starts from right and goes to pivot index
    // while the two indices don't match (not converged)
    while(i <= j) {
        while(collection[i][field] < pivot[field]) {
            i++;
        }
        while(collection[j][field] > pivot[field]) {
            j--;
        }
        // if the two indices still don't match, swap the values
        if(i <= j) {
            this.swap(collection, i, j); // only thing changes between orders
            // change indices to continue loop
            i++;
            j--;
        }
    }
    return i;
}

/*
    Quicksort implementation.
    collection (array of objects) is sorted in place
    field (key/val) object is the property of each object by which to sort on,
    -1 for descending, 1 for ascending eg: {_id:-1}
*/
UTIL.quickSort = function (collection, sort, left, right) {
    var index;
    var field;
    if(collection.length > 1) {
        // TODO: Find better way of getting field since this function is called recusively
        field = Object.keys(sort)[0];
        // incase left and right aren't provided
        left = (typeof left != "number" ? 0 : left);
        right = (typeof right != "number" ? collection.length-1 : right);
        // split up the array
        index = this.partition(collection, field, left, right);
        if(left < index-1) {
            this.quickSort(collection, sort, left, index-1);
        }
        if(index < right) {
            this.quickSort(collection, sort, index, right);
        }
    }
    return collection;
}

/*
  Selection-sort implementation
  collection (array of objects) is sorted in place
  field (string) is the property of each object by which to sort on eg: '_id'

  Similar to bubble sort,it uses two loops to accomplish the task,
  ultimately resulting in the O(n2) complexity.
*/
UTIL.selectionSort = function (collection, field) {
    var len = collection.length;
    var min, i, j;
    for(i = 0; i < len; i++) {
        // set minimum to this position
        min = i;
        //check the rest of the array to see if anything is smaller
        for(j = i+1; j < len; j++) {
            if(collection[j][field] < collection[min][field]) {
                min = j;
            }
        }
        // if the minimum isn't in the position, swap it
        if(i != min) {
            this.swap(collection, i, min);
        }
    }
    return collection;
}

// Reverse an array in place
// Aka 'for push then splice' - Extremely slow on v8,
// native reverse() is very well optimized in C/Assembly and uses memory blocked scoping
// Benchmarks: http://jsperf.com/js-array-reverse-vs-while-loop/5
// The built in array.reverse method is ~ 97% slower
UTIL.reverseList = function (list) {
    var length = list.length;
    for(length -= 2; length > -1; length -= 1) {
          list.push(list[length]);
          list.splice(length, 1);
    }
    return list;
}

// return new copied list
// Very fast but still slower than native array.slice() method
UTIL.copyList = function (list) {
    var i = list.length;
    var b = [];
    while (i--) b[i] = list[i];
    return b;
}

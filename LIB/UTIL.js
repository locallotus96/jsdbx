'use strict';

//-- UTILITIES - Helper Functions

var UTIL = {}; // utility object (class)

/*
    A helper function to return whether or not a document contains
    all fields and values in the query.
*/
UTIL.matchAll = function (rec, query) {
    for(var p in query) {
        /*if(!(p in rec) || !(rec[p] === query[p]))) {
            return false; // a field didn't match
        }*/
        // faster ~ 25%
        if(!(rec[p] === query[p])) {
            return false; // a field didn't match
        }
    }
    return true; // all fields match
}
// Like matchAll but for a single field (~ inverse)
UTIL.matchOne = function (rec, query) {
    for(var p in query) {
        /*if(p in rec && rec[p] === query[p]) {
            return true; // a field matches
        }*/
        // faster ~ 25%
        if(rec[p] === query[p]) {
            return true; // a field matches
        }
    }
    return false; // no fields match
}

UTIL.getObjectSize = function (obj) {
    return Object.keys(obj).length;
}

// Method to remove all nullified objects from a list
UTIL.filterDeleted = function (list) {
    for(var i = 0; i < list.length; i++) {
        if(list[i]._id === null) {
            list.splice(list[i], 1);
        }
    }
    return list;
}

// Method to return whether or not a list contains an object
UTIL.listContains = function (list, obj) {
    for(var i = 0; i < list.length; i++) {
        if(list[i] === obj) {
            return true;
        }
    }
    return false;
}

/*
  Is there some method of array like set in python?
  The standard way to do this is usually insert elements into a hash,
  then collect the keys - since keys are guaranteed to be unique.
  Or, similarly, but preserving order:
*/
// Slightly more consistent algorithm performance-wise
UTIL.getUniqueElements = function (collection) {
    var seen = {}; // set containing unique elements
    var result = [];
    var elem, elemStr;
    for (var i = 0; i < collection.length; i++) {
        elem = collection[i];
        // stringify the object it so it can be a valid unique property in the set
        elemStr = JSON.stringify(elem);
        if (!seen[elemStr]) {
            seen[elemStr] = true;
            result[result.length] = elem;
        }
    }
    return result;
}

// Removes duplicate elements from a collection / array
// Use the notion of a set, which every JavaScript object is an example of.
// Slightly faster on arrays with more unique elements compared to getUniqueElements()
// When not many unique elements are present, this is slower
// Slows down again when nearly all elements are unique, catch 22...
UTIL.deduplicate = function (collection) {
    var set = {};
    for(var i = 0; i < collection.length; i++) {
        // adds or replaces an entry in the set
        // stringify objects so they can be used as valid unique properties in the set
        // redundant for strings and numbers but still needed due to parse
        set[JSON.stringify(collection[i])] = true;
    }
    // Put all properties of the set back into the array
    collection = [];
    for(var elem in set) { // awlays gets the element as a string, so we need to parse it
        collection[collection.length] = JSON.parse(elem);
    }
    return collection;
}

/*
   Binary Search implementation for a sorted collection (array) of objects
   where each object contains a particular field that it's being indexed/sorted on, thus searched on
   The function takes an array of objects, and an object with a single key:value pair, the field/key must match the name
   and data type of the of the field each object is sorted on in the collection
   The function returns an array with the object found, and the index it was found at in the collection

   Params: collection:array[object, object..], searchKey:object{field:value}
   Returns: array[object, index]

   NOTE: The array to search through needs to be sorted
   Query aka Key is the field to search for, it assumes that the collection
   is sorted based on the field that this key represents
   ie: If the key is _id, the collection needs to be sorted by _id
*/
UTIL.binarySearch = function (collection, query) {
    var searchKey = Object.keys(query)[0];
    var searchVal = query[searchKey];
    var iMin = 0;
    var iMax = collection.length-1;
    var iMid;
    var iterations = 0;
    console.log(':: UTIL.binarySearch Collection:', collection.length + ' documents, Search Key/Val:', searchKey + ':', searchVal);
    while(iMin < iMax) {
        /*
        Selecting a pivot (middle) element is also complicated by the existence of integer overflow.
        If the boundary indices of the subarray being sorted are sufficiently large,
        the naïve expression for the middle index, (lo + hi)/2, will cause overflow and provide an invalid pivot index.
        This can be overcome by using, for example, lo + (hi−lo)/2 to index the middle element, at the cost of more complex arithmetic.
        */
        // calculate the midpoint for roughly equal partition
        //iMid = Math.floor((iMin + iMax) / 2);
        iMid = Math.floor(iMin + (iMax - iMin) / 2);
        iterations++;
        console.log(':: UTIL.binarySearch Checking Key:', collection[iMid][searchKey]);
        if(searchVal < collection[iMid][searchKey]) {
            iMax = iMid - 1;
        } else if(searchVal > collection[iMid][searchKey]) {
            iMin = iMid + 1;
        } else {
            console.log(':: UTIL.binarySearch Depth:', iterations + ', Found Node with Key/Val:', searchKey + ':', searchVal);
            return [collection[iMid], iMid]; // return the object at collection index iMid, and iMid
        }
    }
    console.log(':: UTIL.binarySearch Depth:', iterations + ', Could not find key');
    return [];
}

/*
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
*/
UTIL.partition = function (collection, field, left, right) {
    /*
        Selecting a pivot (middle) element is also complicated by the existence of integer overflow.
        If the boundary indices of the subarray being sorted are sufficiently large,
        the naïve expression for the middle index, (lo + hi)/2, will cause overflow and provide an invalid pivot index.
        This can be overcome by using, for example, lo + (hi−lo)/2 to index the middle element, at the cost of more complex arithmetic.
    */
    //var pivot = collection[Math.floor((right + left) / 2)]; // middle index
    var pivot = collection[Math.floor(left + (right - left) / 2)][field]; // middle index
    var i = left; // starts from left and goes to pivot index
    var j = right; // starts from right and goes to pivot index
    // while the two indices don't match (not converged)
    while(i <= j) {
        while(collection[i][field] < pivot) {
            i++;
        }
        while(collection[j][field] > pivot) {
            j--;
        }
        // if the two indices still don't match, swap the values
        if(i <= j) {
            this.swap(collection, i, j);
            // change indices to continue loop
            i++;
            j--;
        }
    }
    // necessary for recursion
    return i;
}

/*
    Quicksort implementation.
    collection (array of objects) is sorted in place
    field (string) is the property of each object by which to sort on eg: '_id'
*/
// TODO: Does it still work?
UTIL.quickSort = function (collection, field, left, right) {
    var index;
    if(collection.length > 1) {
        // incase left and right aren't provided
        left = (typeof left != "number" ? 0 : left);
        right = (typeof right != "number" ? collection.length-1 : right);
        // split up the array
        index = this.partition(collection, field, left, right);
        if(left < index-1) {
            this.quickSort(collection, field, left, index-1);
        }
        if(index < right) {
            this.quickSort(collection, field, index, right);
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
// TODO: Does it still work?
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

module.exports = UTIL;

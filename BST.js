//--- Binary Search Tree Implementation, index on document._id
BST = {};
BST._root = null;
BST.insert = function (object) {
    var node = { // node to insert
        object: object,
        left: null, // pointer to left node
        right: null // pointer to right node
    }
    var current; // pointer to current node in tree
    if(this._root === null) {
        this._root = node;
    } else {
        current = this._root;
        while(true) {
            if(object._id < current.object._id) {
                if(current.left === null) {
                    current.left = node;
                    return true;
                } else {
                    current = current.left;
                }
            } else if(object._id > current.object._id) {
                if(current.right === null) {
                    current.right = node;
                    return true;
                } else {
                    current = current.right;
                }
            } else {
                // if new entry is equal to current, ignore and stop
                // BST cannot have duplicates!
                return false;
            }
        }
    }
}
BST.find = function (object) {
    var found = false;
    var current = this._root;
    var depth = 0;
    while(!found && current) {
        if(object._id < current.object._id) {
            current = current.left;
            depth += 1;
        } else if(object._id > current.object._id) {
            current = current.right;
            depth += 1;
        } else {
            console.log(':: BST Found ID: \n', current);
            found = true;
        }
    }
    console.log(':: BST Search Depth (find):', depth)
    return (current !== null ? current.object : {});
}
BST.contains = function (object) {
    var found = false;
    var current = this._root;
    var depth = 0;
    while(!found && current) {
        if(object._id < current.object._id) {
            current = current.left;
            depth += 1;
        } else if(object._id > current.object._id) {
            current = current.right;
            depth += 1;
        } else {
            console.log(':: BST Contains ID: \n', current);
            found = true;
        }
    }
    console.log(':: BST Search Depth (contains):', depth);
    return found;
}
BST.remove = function (object) {
    var found = false;
    var parent = null;
    var current = this._root;
    var searchKey = object._id;
    var key, value;
    var childCount, replacement, replacementParent;
    while(!found && current) {
        key = current.object._id;
        value = current.object;
        if(searchKey < key) {
            parent = current;
            current = current.left;
        } else if(searchKey > key) {
            parent = current;
            current = current.right;
        } else {
            found = true;
        }
    }
    if(found) {
        // figure out how many children
        childCount = (current.left !== null ? 1 : 0)
            + (current.right !== null ? 1 : 0);
        // special case, found node is root node
        if(current === this._root) {
            switch(childCount) {
                // root has no children, simply erase
                case 0:
                    this._root = null;
                    break;
                // root has one child, use one as new root
                case 1:
                    this._root = (current.right === null ? current.left : current.right);
                    break;
                // root has two children, takes some work
                case 2:
                    // new root will be old root's left child, maybe
                    replacement = this._root.left;
                    // find the right-most leaf node to be the real new root
                    while(replacement.right !== null) {
                        replacementParent = replacement;
                        replacement = replacement.right;
                    }
                    // it's not the first node on the left
                    if(replacementParent !== null) {
                        // remove the new root from it's previous position
                        replacementParent.right = replacement.left;
                        // give the new root all the old root's children
                        replacement.right = this._root.right;
                        replacement.left = this._root.left;
                    } else {
                        // just assign the children
                        replacement.right = this._root.right;
                    }
                    // officially assign new root
                    this._root = replacement;
            }
        // found node is not root node
        } else {
            switch(childCount) {
                // no children, just remove it from the parent
                case 0:
                    if(key < parent.object._id) {
                        parent.left = null;
                    } else {
                        parent.right = null;
                    }
                    break;
                // one child, just reassign to parent
                case 1:
                    if(key < parent.object._id) {
                        parent.left = (current.left === null ? current.right : current.left);
                    } else {
                        parent.right = (current.left === null ? current.right : current.left);
                    }
                    break;
                // two children, a bit more complicated
                case 2:
                    // reset pointers for new traversal
                    replacement = current.left;
                    replacementParent = current;
                    // find the right-most node
                    while(replacement.right !== null) {
                        replacementParent = replacement;
                        replacement = replacement.right;
                    }
                    replacementParent.right = replacement.left;
                    // assign children to the replacement
                    replacement.right = current.right;
                    replacement.left = current.left;
                    // place the replacement in the right spot
                    if(key < parent.object._id) {
                        parent.left = replacement;
                    } else {
                        parent.right = replacement;
                    }
            }
        }
    }
}
// Generic traversal function
// Process is a function that should be run on each node in the tree
// This method is used to implement size(), toArray() and toString()
BST.traverse = function (process) {
    // recursive helper function for in order traversal
    function inOrder(node) {
        if(node) {
            // traverse the left subtree
            if(node.left !== null) {
                inOrder(node.left);
            }
            // call the process method on this node
            process.call(this, node);
            // traverse the right subtree
            if(node.right !== null) {
                inOrder(node.right);
            }
        }
    }
    // start with root node
    inOrder(this._root);
}
BST.size = function () {
    var length = 0;
    this.traverse(function (node) {
        length ++;
    });
    return length;
}
BST.toArray = function () {
    var result = [];
    this.traverse(function (node) {
       result.push(node.object);
    });
    return result;
}
BST.toString = function () {
    return this.toArray().toString();
}

//--- Binary Search Tree Implementation, key/index on document.name, store array indices with this name as value
BST2 = {};
BST2._root = null;
BST2.insert = function (key, value) {
    var node = { // node to insert
        key: key, // sort this tree by document.name
        value: [value], // array indices of documents with this name, now we can collection[index] instead of looping
        left: null, // pointer to left node
        right: null // pointer to right node
    }
    var current; // pointer to current node in tree
    if(this._root === null) {
        this._root = node;
    } else {
        current = this._root;
        while(true) {
            if(key < current.key) {
                if(current.left === null) {
                    current.left = node;
                    return true;
                } else {
                    current = current.left;
                }
            } else if(key > current.key) {
                if(current.right === null) {
                    current.right = node;
                    return true;
                } else {
                    current = current.right;
                }
            } else {
                // if new entry is equal to current, add the index
                // unless index exists
                if(value in current.value) {
                    return false;
                } else {
                    current.value.push(value);
                    return true;
                }
            }
        }
    }
}
BST2.find = function (key) {
    var found = false;
    var current = this._root;
    var depth = 0;
    while(!found && current) {
        if(key < current.key) {
            current = current.left;
            depth += 1;
        } else if(key > current.key) {
            current = current.right;
            depth += 1;
        } else {
            console.log(':: BST2 Found Key: \n', current);
            found = true;
        }
    }
    console.log(':: BST2 Search Depth (find):', depth)
    return (current !== null ? current.value : []);
}
BST2.contains = function (key) {
    var found = false;
    var current = this._root;
    var depth = 0;
    while(!found && current) {
        if(key < current.key) {
            current = current.left;
            depth += 1;
        } else if(key > current.key) {
            current = current.right;
            depth += 1;
        } else {
            console.log(':: BST2 Contains Key: \n', current);
            found = true;
        }
    }
    console.log(':: BST2 Search Depth (contains):', depth)
    return found;
}
BST2.remove = function (searchKey) {
    var found = false;
    var parent = null;
    var current = this._root;
    var childCount, replacement, replacementParent;
    while(!found && current) {
        if(searchKey < current.key) {
            parent = current;
            current = current.left;
        } else if(searchKey > current.key) {
            parent = current;
            current = current.right;
        } else {
            found = true;
        }
    }
    if(found) {
        // figure out how many children
        childCount = (current.left !== null ? 1 : 0)
            + (current.right !== null ? 1 : 0);
        // special case, found node is root node
        if(current === this._root) {
            switch(childCount) {
                // root has no children, simply erase
                case 0:
                    this._root = null;
                    break;
                // root has one child, use one as new root
                case 1:
                    this._root = (current.right === null ? current.left : current.right);
                    break;
                // root has two children, takes some work
                case 2:
                    // new root will be old root's left child, maybe
                    replacement = this._root.left;
                    // find the right-most leaf node to be the real new root
                    while(replacement.right !== null) {
                        replacementParent = replacement;
                        replacement = replacement.right;
                    }
                    // it's not the first node on the left
                    if(replacementParent !== null) {
                        // remove the new root from it's previous position
                        replacementParent.right = replacement.left;
                        // give the new root all the old root's children
                        replacement.right = this._root.right;
                        replacement.left = this._root.left;
                    } else {
                        // just assign the children
                        replacement.right = this._root.right;
                    }
                    // officially assign new root
                    this._root = replacement;
            }
        // found node is not root node
        } else {
            switch(childCount) {
                // no children, just remove it from the parent
                case 0:
                    if(current.key < parent.key) {
                        parent.left = null;
                    } else {
                        parent.right = null;
                    }
                    break;
                // one child, just reassign to parent
                case 1:
                    if(current.key < parent.key) {
                        parent.left = (current.left === null ? current.right : current.left);
                    } else {
                        parent.right = (current.left === null ? current.right : current.left);
                    }
                    break;
                // two children, a bit more complicated
                case 2:
                    // reset pointers for new traversal
                    replacement = current.left;
                    replacementParent = current;
                    // find the right-most node
                    while(replacement.right !== null) {
                        replacementParent = replacement;
                        replacement = replacement.right;
                    }
                    replacementParent.right = replacement.left;
                    // assign children to the replacement
                    replacement.right = current.right;
                    replacement.left = current.left;
                    // place the replacement in the right spot
                    if(current.key < parent.key) {
                        parent.left = replacement;
                    } else {
                        parent.right = replacement;
                    }
            }
        }
    }
}
// Generic traversal function
// Process is a function that should be run on each node in the tree
// This method is used to implement size(), toArray() and toString()
BST2.traverse = function (process) {
    // recursive helper function for in order traversal
    function inOrder(node) {
        if(node) {
            // traverse the left subtree
            if(node.left !== null) {
                inOrder(node.left);
            }
            // call the process method on this node
            process.call(this, node);
            // traverse the right subtree
            if(node.right !== null) {
                inOrder(node.right);
            }
        }
    }
    // start with root node
    inOrder(this._root);
}
BST2.size = function () {
    var length = 0;
    this.traverse(function (node) {
        length ++;
    });
    return length;
}
BST2.toArray = function () {
    var result = [];
    this.traverse(function (node) {
       result.push(node.value);
    });
    return result;
}
BST2.toString = function () {
    return this.toArray().toString();
}

exports.BST = BST;
exports.BST2 = BST2;

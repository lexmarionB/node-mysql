var conn = require('../setup/dbConnection.js'),
    connPool = require('../setup/dbConnectionPool.js'),
    serialize = require('./serialize.js'),
    unserialize = require('./unserialize.js'),
    util = require('./utilities.js'),
    Database = function(params) {
        this.database = params.database;
        this.table = params.table;
        this.fields = params.fields ? params.fields : {};
        this.engine = params.engine ? params.engine : 'InnoDB';
        this.fulltext = params.fulltext ? params.fulltext : [];
        this.prefill = params.prefill ? params.prefill : [];
        this.last_query = '';

        this.insertId = 0;
        this.id = 0;
        this.affectedRows = 0;
        this.changedRows = 0;

        this.init();
};

Database.prototype.autoCreate = function() {
    if(this.table && this.table !== '') {
        this.create(this.sync);
    }
};

Database.prototype.get_table = function() {
    return this.table;
};

Database.prototype.get_fields = function() {
    var fields = Object.keys(this.fields),
        ki = fields.indexOf('key');

    if(ki !== -1) {
        delete fields[ki];
    }

    return fields;
};

Database.prototype.init = function() {
    var id = false,
        date_created = false,
        date_modified = false,
        field;

    for(field in this.fields) {
        if(this.fields.hasOwnProperty(field)) {
            switch(field) {
                case 'id':
                    id = true;
                    break;
                case 'date_created':
                    date_created = true;
                    break;
                case 'date_modified':
                    date_modified = true;
                    break;
            }
        }
    }

    if(! id) {
        this.fields.id = 'INT(11) NOT NULL AUTO_INCREMENT';
    }
    if(! date_created) {
        this.fields.date_created = "DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'";
    }
    if(! date_modified) {
        this.fields.date_modified = "DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'";
    }
};

Database.prototype.create = function(cb) {
    var that = this,
        fields = "",
        index = 0,
        field;

    for(field in this.fields) {
        if(this.fields.hasOwnProperty(field)) {
            var attr = this.fields[field], key;

            if(index) {
                fields += ", ";
            }
            if(field == 'fulltext') {
                fields += field.toUpperCase() + " (`" + attr.split(",").join('`,`') + "`)";
                this.fulltext = attr.split(",");

                for(key in this.fulltext) {
                    if(this.fulltext.hasOwnProperty(key)) {
                        this.fulltext[key] = trim(this.fulltext[key]);
                    }
                }
            } else if(field == 'key') {
                fields += attr;
            } else {
                fields += '`' + field + "` " + attr;
            }
            index++;
        }
    }

    delete this.fields.key;
    delete this.fields.fulltext;

    return this.query("CREATE TABLE IF NOT EXISTS " + this.table + " (" + fields + ") ENGINE=" + this.engine, function() {
        if(cb) {
            cb.call(that);
        }
    });
};

Database.prototype.sync = function() {
    var that = this,
        columns = that.get_struct(),
        field, found, updated;

    //search columns to remove
    columns.forEach(function(column) {
        found = false;

        for(field in that.fields) {
            if(that.fields.hasOwnProperty(field)) {
                if(column.COLUMN_NAME == field) {
                    found = true;
                }
            }
        }

        if(! found) {
            that.remove_field(column.COLUMN_NAME);
        }
    });

    for(field in this.fields) {
        if(this.fields.hasOwnProperty(field)) {
            var attr = this.fields[field];
            found = false;
            updated = true;

            columns.forEach(function(column, index) {
                if(column.COLUMN_NAME == field) {
                    var db_attr = column.COLUMN_TYPE;
                    found = true;

                    if(column.IS_NULLABLE == 'NO') {
                        db_attr += " NOT NULL";
                    }
                    if(column.COLUMN_DEFAULT !== null) {
                        db_attr += " DEFAULT '" + column.COLUMN_DEFAULT + "'";
                    }
                    if(! util.empty(column.EXTRA)) {
                        db_attr += " " + column.EXTRA;
                    }

                    if(attr.toLowerCase() != db_attr.toLowerCase()) {
                        updated = false;
                    }

                    if(column.INDEX_TYPE != 'FULLTEXT' && that.fulltext.indexOf(column.COLUMN_NAME) >= 0) {
                        that.add_fulltext(column.COLUMN_NAME);
                    } else if(column.INDEX_TYPE == 'FULLTEXT' && that.fulltext.indexOf(column.COLUMN_NAME) < 0) {
                        that.remove_fulltext(column.COLUMN_NAME);
                    }

                    delete columns[index];

                    return;
                }
            });

            if(! found) {
                this.add_field(field, attr);
                if(this.fulltext.indexOf(field) >= 0) {
                    this.add_fulltext(field);
                }
            } else if(! updated) {
                this.update_field(field, attr);
            }
        }
    }

    if(this.get_engine() != this.engine) {
        this.set_engine(this.engine);
    }

    // process prefill
    this.prefill.forEach(function(entry) {
        that.save(entry, function(){});
    });
};

Database.prototype.get_struct = function(params) {
    var database = util.empty(params) || util.empty(params.database) ? this.database : params.database,
        table = util.empty(params) || util.empty(params.table) ? this.table : params.table,
        columns = this.query("SELECT DISTINCT COLUMNS.COLUMN_NAME, COLUMNS.COLUMN_TYPE, COLUMNS.IS_NULLABLE, COLUMNS.COLUMN_DEFAULT, COLUMNS.EXTRA, STATISTICS.INDEX_TYPE FROM INFORMATION_SCHEMA.COLUMNS LEFT JOIN INFORMATION_SCHEMA.STATISTICS ON COLUMNS.COLUMN_NAME = STATISTICS.COLUMN_NAME AND COLUMNS.TABLE_NAME = STATISTICS.TABLE_NAME AND COLUMNS.TABLE_SCHEMA = STATISTICS.TABLE_SCHEMA WHERE COLUMNS.TABLE_SCHEMA='" + database + "' AND COLUMNS.TABLE_NAME='" + table + "'");

    if(columns) {
        return columns;
    } else {
        return {};
    }
};

Database.prototype.query = function(query, cb) {
    var that = this, results = false;

    if(typeof cb !== typeof undefined) {
        this.last_query = query;

        //console.log('Open connections: ' + connPool.getOpenConnectionLength());
        connPool.getConnection(function(err, dbConn) {
            if(err) {
                console.log('Connection Error: ' + err);
                that.query(query, cb);
            } else {
                dbConn.query({
                    sql:    query,
                    timeout: 40000
                }, function(error, results, fields) {
                    if(error) {
                        console.log('Query Error: ' + error.stack);
                        return;
                    }

                    if(results.insertId) {
                        that.insertId = results.insertId;
                        that.id = results.insertId;
                    }

                    if(results.affectedRows) {
                        that.affectedRows = results.affectedRows;
                    }

                    if(results.changedRows) {
                        that.changedRows = results.changedRows;
                    }

                    dbConn.destroy();

                    if(typeof cb === typeof function(){}) {
                        cb(results, that);
                    }
                });
            }
        });
    } else {
        this.last_query = query;
        results = conn.query(query);

        if(results) {
            return results;
        }
    }

    return false;
};

Database.prototype.set_engine = function(engine) {
    return this.query("ALTER TABLE `" + this.table + "` ENGINE=" + engine)
};

Database.prototype.get_engine = function() {
    var results = this.query("SELECT ENGINE FROM INFORMATION_SCHEMA.TABLES WHERE `TABLE_NAME`='" + this.table + "' LIMIT 1");

    return results[0].ENGINE;
};

Database.prototype.add_fulltext = function(column) {
    return this.query("ALTER TABLE " + this.database + "." + this.table + " ADD FULLTEXT(`" + column + "`)");
};

Database.prototype.remove_fulltext = function(column) {
    return this.query("ALTER TABLE " + this.database + "." + this.table + " DROP INDEX " + column);
};

Database.prototype.get_fulltext = function() {
    return this.fulltext;
};

Database.prototype.add_field = function(column, attr) {
    return this.query("ALTER TABLE `" + this.table + "` ADD `" + column + "` " + attr);
};

Database.prototype.remove_field = function(column) {
    return this.query("ALTER TABLE `" + this.table + "` DROP `" + column + "`");
};

Database.prototype.update_field = function(column, attr) {
    return this.query("ALTER TABLE `" + this.table +  "` MODIFY `" + column + "` " + attr);
};

Database.prototype.order = function(params) {
    var order = '',
        orders = [],
        field;

    if(! util.empty(params)) {

        if(params.constructor == Object) {
            for(field in params) {
                if(params.hasOwnProperty(field)) {
                    orders.push('`' + field + '` ' + params[field]);
                }
            }

            order = 'ORDER BY ' . orders.join(",");
        } else if(params.constructor == Array) {
            if(params[0].constructor == Array) {
                params[0] = params[0].join(",");
            }

            order = 'ORDER BY `' + params[0] + '` ' + params[1];
        } else {
            order = 'ORDER BY `' + params + '`';
        }
    }

    return order;
};

Database.prototype.clause = function(key, value) {
    var clause = '',
        numeric = true,
        date = true,
        like = false,
        collate = false,
        index, k, v, tmp, conjoin, date_parse;

    if(value.constructor == Object) {
        clause += '(';
        index = 0;

        for(k in value) {
            v = value[k];
            tmp = key;

            if(util.empty(key)) {
                key = k;
            }

            conjoin = this.conjunction(key);
            if(key.indexOf('/') === 0) {
                key = key.replace(/^\/+/, '');
            }
            if(index) {
                clause += conjoin;
            }
            clause += this.clause(key, v);
            index++;

            key = tmp;
        }
        clause += ')';
    } else if(value.constructor == Array) {
        value.forEach(function (v, index) {
            if (isNaN(v) || typeof v === 'string' || v instanceof String) {
                numeric = false;
            }

            date_parse = Date.parse(v);
            if (!isNaN(date) && isNaN(date_parse)) {
                date = false;
            }

            if (v != '' && (v[0] == '%' || v[v.length - 1] == '%')) {
                like = true;

                if (v[1] == '%' || v[v.length - 2] == '%') {
                    collate = true;
                    value[index] = v.slice(1, -1);
                }
            }
        });

        if (value.length == 2 && (numeric || date)) {
            clause += "`" + key + "` BETWEEN '" + this.sanitize(value[0]) + "' AND '" + this.sanitize(value[1]) + "'";
        } else if (like) {
            clause += '(';
            value.forEach(function (v, index) {
                if (index) {
                    clause += ' OR ';
                }
                clause += "`" + key + "` LIKE '" + this.sanitize(v) + "'";
                if (collate) {
                    clause += " COLLATE utf8_general_ci";
                }
            });
            clause += ')';
        } else {
            let that = this;

            clause += "`" + key + "` IN(";
            value.forEach(function (v, index) {
                if (index) {
                    clause += ",";
                }
                clause += "'" + that.sanitize(v) + "'";
            });
            clause += ")";
        }
    } else if(value.constructor == Number) {
        clause += "`" + key + "`='" + this.sanitize(value.toString()) + "'";
    } else {
        if(value != '' && (value[0] == '%' || value[value.length - 1] == '%')) {
            clause += "`" + key + "` LIKE '" + this.sanitize(value) + "'";
        } else if(value.indexOf('MATCH') === 0) {
            value = value.slice(6);
            clause += 'MATCH (`' + key + "`) AGAINST ('" + this.sanitize(value) + "' IN NATURAL LANGUAGE MODE)";
        } else if(value.indexOf('gt;') === 0) {
            value = value.slice(3);
            clause += "`" + key + "`>='" . this.sanitize(value) + "'";
        } else if(value.indexOf('lt;') === 0) {
            value = value.slice(3);
            clause += "`" + key + "`<='" + this.sanitize(value) + "'";
        } else {
            clause += "`" + key + "`='" + this.sanitize(value) + "'";
        }
    }

    return clause;
};

Database.prototype.conjuction = function(key) {
    var conjoin = ' AND ';
    if(key.indexOf('/') === 0) {
        conjoin = ' OR ';
    }
    return conjoin;
};

Database.prototype.where = function(params) {
    var where = '',
        index = 0,
        key, conjoin;
    if(! util.empty(params)) {
        index = 0;
        where = 'WHERE ';

        for(key in params) {
            if(params.hasOwnProperty(key)) {
                conjoin = this.conjuction(key);
                if(index) {
                    where += conjoin;
                }
                if(key.indexOf('/') === 0) {
                    key = key.replace(/^\/+/, '');
                }
                where += this.clause(key, params[key]);
                index++;
            }
        }
    }

    return where;
};

Database.prototype.validate = function(params) {
    var new_params = {},
        key, vals, field;

    for(key in params) {
        if(params.hasOwnProperty(key)) {
            vals = key.split(".");
            for(field in this.fields) {
                if(key == field || (key.indexOf('/') === 0 && key.replace(/^\/+/, '') == field) || (! util.empty(vals) && vals.length == 2)) {
                    new_params[key] = params[key];
                    break;
                }
            }
        }
    }

    return new_params;
};

Database.prototype.filter = function(variable) {
    return variable !== null && variable !== false;
};

Database.prototype.params = function(query, params, order, limit) {
    limit = util.empty(limit) ? 0 : limit;

    if(! isNaN(params) && typeof params !== typeof true) {
        query += " WHERE id='" + params + "' LIMIT 1";
        return query;
    }

    if((isNaN(params) && typeof params !== typeof true) && ! (typeof params === 'string' || params instanceof String)) {
        params = this.validate(params);
        query += ' ' + this.where(params);
    }

    query += ' ' + this.order(order);

    if(! util.empty(limit)) {
        query += " LIMIT ";

        if(limit.constructor == Array) {
            query += limit[0] + ', ' + limit[1];
        } else {
            query += limit;
        }
    }

    return query;
};

Database.prototype.sanitize = function(param) {
    var k;

    if(param === undefined) {
        return '';
    } else if(param.constructor == String) {
        param = param.replace(/\\(.)/mg, "$1").replace(/\\\\/mg, '\\\\\\\\');
        param = param.replace(/\"/mg, '"').replace(/\'/mg, "''");
    } else {
        for(k in param) {
            if(param.constructor == Object && ! param.hasOwnProperty(k)) {
                continue;
            }

            param[k] = this.sanitize(param[k]);
        }
    }

    return param;
};

Database.prototype.distinct = function(fields, params, order, group, limit, cb) {
    var field, query, results, arr = [];

    fields = util.empty(fields) ? [] : fields;
    params = params ? params : {};
    order = util.empty(order) ? ['date_created', 'DESC'] : order;
    group = util.empty(group) ? [] : group;
    limit = util.empty(limit) ? 0 : limit;

    field = fields;
    if(fields.constructor == Array) {
        field = fields.join(",");
    }

    query = "SELECT DISTINCT " + field + " FROM " + this.table;

    query = this.params(query, params, order, limit);
    if(! util.empty(group)) {
        query = query.replace('ORDER BY ', 'GROUP BY ' + group.join(",") + ' ORDER BY ');
    }

    results = this.query(query, cb);

    if(fields.constructor == Array || util.empty(results)) {
        return results;
    } else {
        results.forEach(function(r) {
            arr.push(r.field);
        });

        return arr;
    }
};

Database.prototype.select = function(params, order, limit, cb) {
    params = util.empty(params) ? false : params;
    order = util.empty(order) ? ['date_created', 'DESC'] : false;
    limit = util.empty(limit) ? 0 : limit;

    var query, k;

    query = 'SELECT * FROM ' + this.table;

    if(! isNaN(params) && typeof params !== typeof true) {
        query += ' WHERE id="' + params + '" LIMIT 1';

        return this.query(query, cb)[0];
    }

    query = this.params(query, params, order, limit);

    return this.query(query, cb);
};

Database.prototype.count = function(params, field, limit) {
    var query;

    limit = limit ? limit : 0;

    if(field) {
        query = "SELECT " + field + ", COUNT(" + field + ") as count FROM " + this.table + " GROUP BY " + field;
        query = this.params(query, false, ['count', 'DESC'], limit);
        return this.query(query);
    }

    query = "SELECT COUNT(*) FROM `" + this.table + "` " + this.where(params);
    return this.query(query);
};

Database.prototype.sum = function(params, field) {
    if(util.empty(field)) {
        return this.query("SELECT SUM(" + field + ") as sum FROM `" + this.table + "` " + this.where(params));
    }

    return this.query("SELECT SUM(*) FROM `" + this.table + "` " + this.where(params));
};

Database.prototype.insert = function(params, cb) {
    if(! util.empty(params)) {
        var columns = [], values = [], field, value, unserialized, result;

        for(field in params) {
            if(params.hasOwnProperty(field)) {
                value = params[field];
                columns.push('`' + field + '`');

                try {
                    unserialized = unserialize(value);
                } catch(error) {
                    unserialized = false;
                }

                if(unserialized === false) {
                    values.push("'" + this.sanitize(value) + "'");
                } else {
                    values.push("'" + value + "'");
                }
            }
        }

        return this.query("INSERT INTO " + this.table + "(" + columns.join(", ") + ") VALUES (" + values.join(", ") + ")", cb);
    }

    return false;
};

Database.prototype.update = function(params, values, cb) {
    params = util.empty(params) ? 0 : params;

    if(values.constructor == Object && ! util.empty(values)) {
        var query = "UPDATE " + this.table + " SET ", index = 0, field, value, unserialized;

        for(field in values) {
            if(values.hasOwnProperty(field)) {
                value = values[field];
                if(index) {
                    query += ", ";
                }

                try {
                    unserialized = unserialize(value);
                } catch(error) {
                    unserialized = false;
                }

                if(unserialized === false) {
                    query += "`" + field + "`='" + this.sanitize(value) + "'";
                } else {
                    query += "`" + field + "`='" + value + "'";
                }
                index++;
            }
        }

        if(! isNaN(params) && typeof params !== typeof true) {
            query += " WHERE `id`='" + params + "'";
        } else if(params.constructor == Object && ! util.empty(params)) {
            query += " " + this.where(params);
        }

        this.id = +params;
        return this.query(query, cb);
    }

    return false;
};

Database.prototype.delete = function(params, cb) {
    if(! util.empty(params)) {
        if(! isNaN(params) && typeof params !== typeof true) {
            return this.query("DELETE FROM `"  + this.table + "` WHERE `id`='" + params + "'", cb);
        }

        if(params.constructor == Object) {
            return this.query("DELETE FROM `" + this.table + "` " + this.where(params), cb);
        }
    }

    return false;
};

Database.prototype.truncate = function() {
    return this.query("TRUNCATE TABLE " + this.table);
};

Database.prototype.drop = function() {
    return this.query("DROP TABLE " + this.table);
};

Database.prototype.save = function(params, cb) {
    var id = 0, date = new Date();

    if(! util.empty(params.id)) {
        id = params.id;
    }

    delete params.id;

    if(util.empty(params.date_modified)) {
        params.date_modified = date.getFullYear() + '-' + (date.getMonth() + 1) + '-' + date.getDate() + ' ' + (date.getHours()) + '-' + date.getMinutes() + '-' + date.getSeconds();
    }

    params = this.validate(params);

    if(! util.empty(id)) {
        if(util.empty(this.select(id))) {
            params.id = id;
            params.date_created = params.date_modified;
            return this.insert(params, cb);
        } else {
            return this.update(id, params, cb);
        }
    } else {
        params.date_created = params.date_modified;
        return this.insert(params, cb);
    }
};

Database.prototype.find = function(params, order, limit, cb) {
    return this.select(params, order, limit, cb);
};

module.exports = Database;
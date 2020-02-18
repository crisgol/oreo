var FIREBIRD = require('node-firebird')
var async = require('../async')

var firebird = module.exports

firebird.identifierQuoteChar = ''

var pools = {}

firebird.getClient = function(connSettings, cb) {
  var self = this
  connSettings.multipleStatements = true
 
  var connStr = JSON.stringify(connSettings)
  /*
  if (!pools[connStr]) {
    pools[connStr] = FIREBIRD.pool(5,connSettings)
  }
  var pool = pools[connStr]
*/
// 5 = the number is count of opened sockets
 var pool = FIREBIRD.pool(5,connSettings)
 // Get a free pool
  pool.get(function(err, db) {
  //FIREBIRD.attach(connSettings, function (err, db) {
    if (err) {
      if (!self.db._opts.silent) {
        console.error('pool.getConnection(' + connStr + ')', err)
      }
      return cb(err)
    }
    cb(null, db, ()=>{db.detach()})
  })
}


firebird.end = function(cb) {
  async.each(Object.keys(pools), function (connStr, done) {
    pools[connStr].end(done)
  }, function (err) {
    if (err) return cb(err)
    pools = {}
    cb(null)
  })

  PG.on('end', cb)
  PG.end()
}


//
// for internal use
// don't release the connection back to the pool because
// this function is used within transactions
//
firebird.execute = function(connection, sql, values, cb) {
  if (typeof values === 'function') {
    cb = values
    values = {}
  }
  var self = this
 /* var query = {
    sql: sql,
    typeCast: function (field, next) {
      // bypass MYSQL's conversion of YYYY-MM-DD strings into Date objects
      if (field.type === 'DATE') {
        return field.string();
      }
      return next();
    }
  }
  */

  if (connection._currentTransaction)  // se estiver em transação executar direto na transacao
  {
    self._executeTransaction(connection._currentTransaction, sql, values, cb);
    /*connection._currentTransaction.query(sql,  values
      , function(err, result) {
        if (err) {
          console.log(err)
          return cb(err)
        }
        cb(null, result)
      })*/
  }
  else 
  {
  //executa o metodo nativo de cconsulta ao banco de dados firebird
    connection.query(connection, sql, values, cb);
    /*connection.query(sql,  values
    , function(err, rows) {
      if (err) {
        //console.error(sql)
        return cb(err)
      }
      cb(null, rows)
    })
    */
  }
}



firebird._execute = function(connection, sql, values, cb) {
  if (typeof values === 'function') {
    cb = values
    values = {}
  }
  var self = this
 
  //executa o metodo nativo de cconsulta ao banco de dados firebird
    connection.query(sql,  values
    , function(err, rows) {
      if (err) {
        //console.error(sql)
        return cb(err)
      }
      cb(null, rows)
    })
}

firebird._executeTransaction = function(transaction, sql, values, cb) {
  if (typeof values === 'function') {
    cb = values
    values = {}
  }
  var self = this
 
    transaction.query(sql,  values
      , function(err, result) {
        if (err) {
          console.log(err)
          return cb(err)
        }
        cb(null, result)
      })
  
}


firebird.determineHosts = function(cb) {
  var self = this
  var opts = self.db._opts
  var hosts = {
    readOnly: [],
    writable: []
  }
  // Connect to all hosts and determine read/write servers
  async.each(opts.hosts, function(host, callback) {
    let _host=host.split('/');
    var _port=(_host.length==1?3050:_host[1]); 

   var _pageSize = (opts.pageSize?opts.pageSize:4096);
   var _lowercase_keys = (opts.lowercase_keys?opts.lowercase_keys:false);
   var _role = (opts.role?opts.role:'');
    var sql;
    var conn = {
      host: _host[0],
      port: _port,
      user: opts.user,
      password: opts.pass,
      database: opts.name,
      pageSize: _pageSize,
      lowercase_keys : _lowercase_keys, // set to true to lowercase keys
      role: _role,            // default
  
    }

    sql = [
      'SELECT COUNT(1) SLAVETHREADCOUNT',
      'FROM RDB$DATABASE'
    ].join('\n');

    self.getClient(conn, function(err, db, done) {
      if (err) return callback(err)
      self.execute(db, sql, function(err, rs) {
        
        done();// IMPORTANT: close the connection
        if (err) {
          // try next host
          return callback(null)
        }
        if (rs[0].slavethreadcount > 1) {
          hosts.readOnly.push(conn)
        } else {
          hosts.writable.push(conn)
        }
        callback(null)
      })
    })
  }, function() {
    cb(hosts)
  })
}


firebird.getTables = function(cb) {
  var opts = this.db._opts;
  var _lowercase_keys = (opts.lowercase_keys?opts.lowercase_keys:false);
  var tables = []
  var sql = [
    'SELECT RDB$RELATION_NAME ',
    'FROM RDB$RELATIONS',
    "WHERE RDB$SYSTEM_FLAG = 0 ",
    //" AND RDB$RELATION_NAME = 'AUTHORSB'",
    "ORDER BY RDB$RELATION_NAME"
  ]

  /*var sql = "SELECT RDB$RELATION_NAME from RDB$RELATIONS "+
            "WHERE RDB$SYSTEM_FLAG = 0 "+
            "ORDER BY RDB$RELATION_NAME ";
  */
  this.db.execute(sql, function(err, rs) {
    if (err) return cb(err)
    rs.forEach(function(r) {
      tables.push( (_lowercase_keys?r[Object.keys(r)[0]].toLowerCase():r[Object.keys(r)[0]]))
    })
    cb(null, tables)
  })
}


firebird.getForeignKeys = function(tableName, cb) {
  var opts = this.db._opts;
  var _lowercase_keys = (opts.lowercase_keys?opts.lowercase_keys:false);
  var fk = {}
  var sql = [
    "select ",
    //" PK.RDB$RELATION_NAME as PKTABLE_NAME",
    (_lowercase_keys?' LOWER(PK.RDB$RELATION_NAME)':'PK.RDB$RELATION_NAME '),
    
    " AS PKTABLE_NAME,",
    (_lowercase_keys?' LOWER(ISP.RDB$FIELD_NAME)':'ISP.RDB$FIELD_NAME '),
    
    " as PKCOLUMN_NAME",
    ",FK.RDB$RELATION_NAME as FKTABLE_NAME,",
    (_lowercase_keys?' LOWER(ISF.RDB$FIELD_NAME)':'ISF.RDB$FIELD_NAME '),

    " as FKCOLUMN_NAME",

    ",(ISP.RDB$FIELD_POSITION + 1) as KEY_SEQ",
    ",RC.RDB$UPDATE_RULE as UPDATE_RULE",
    ",RC.RDB$DELETE_RULE as DELETE_RULE",
    ",PK.RDB$CONSTRAINT_NAME as PK_NAME ,",
    //",FK.RDB$CONSTRAINT_NAME as FK_NAME",
    
    (_lowercase_keys?' LOWER(FK.RDB$CONSTRAINT_NAME)':'FK.RDB$CONSTRAINT_NAME '),
    "  as FK_NAME from",
    "RDB$RELATION_CONSTRAINTS PK",
    ",RDB$RELATION_CONSTRAINTS FK",
    ",RDB$REF_CONSTRAINTS RC",
    ",RDB$INDEX_SEGMENTS ISP",
    ",RDB$INDEX_SEGMENTS ISF",
    "WHERE FK.RDB$RELATION_NAME = '" + (_lowercase_keys?tableName.toUpperCase():tableName) + "' and",
    " FK.RDB$CONSTRAINT_NAME = RC.RDB$CONSTRAINT_NAME ",
    "and PK.RDB$CONSTRAINT_NAME = RC.RDB$CONST_NAME_UQ ",
    "and ISP.RDB$INDEX_NAME = PK.RDB$INDEX_NAME ",
    "and ISF.RDB$INDEX_NAME = FK.RDB$INDEX_NAME ",
    "and ISP.RDB$FIELD_POSITION = ISF.RDB$FIELD_POSITION ",
    "order by 1, 5 DESC "
    
  ]
  this.db.execute(sql, function(err, rs) {
    if (err) return cb(err)   /*todo cristiano*/
    
    let _columns=[];        //array de colunas
    let _foreignColumns=[];  //array de colunas

    rs.forEach(function(r) {
        // parse r.condef
        //var regExp = /\(([^)]+)\) REFERENCES (.+)\(([^)]+)\)/;
        //var matches = regExp.exec(r.condef);
        _columns.push(r.fkcolumn_name)
        _foreignColumns.push(r.pkcolumn_name)

        if (r.key_seq===1)
        {
          fk[r.fk_name] = {
            constraintName: r.fk_name,
            table: tableName,
            columns: _columns,        //array de colunas
            foreignTable: r.pktable_name,
            foreignColumns: _foreignColumns  //array de colunas
          }
           _columns=[];        //array de colunas
          _foreignColumns=[];  //array de colunas
      
        }
      })
      cb(null, fk)
    })
}


firebird.getColumns = function(tableName, cb) {
  var opts = this.db._opts;
  var _lowercase_keys = (opts.lowercase_keys?opts.lowercase_keys:false);
  var columns = []
  var sql = [
    'SELECT',
 // 'RF.RDB$RELATION_NAME TABLE_NAME,',
  //'RF.RDB$FIELD_NAME FIELD_NAME,',
  
  (_lowercase_keys?' LOWER(RF.RDB$FIELD_NAME) AS NAME,':'RF.RDB$FIELD_NAME AS NAME,'),

 // 'RF.RDB$FIELD_POSITION FIELD_POSITION,',
  "IIF(SUBSTRING(RF.RDB$FIELD_SOURCE FROM 1 FOR 4)='RDB$' ,",
  'CASE F.RDB$FIELD_TYPE',
    'WHEN 7 THEN',
      'CASE F.RDB$FIELD_SUB_TYPE',
        "WHEN 0 THEN 'SMALLINT'",
        "WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'",
        "WHEN 2 THEN 'DECIMAL'",
      'END',
   'WHEN 8 THEN',
      'CASE F.RDB$FIELD_SUB_TYPE',
        "WHEN 0 THEN 'INTEGER'",
        "WHEN 1 THEN 'NUMERIC('  || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'",
        "WHEN 2 THEN 'DECIMAL'",
      'END',
    "WHEN 9 THEN 'QUAD'",
    "WHEN 10 THEN 'FLOAT'",
    "WHEN 12 THEN 'DATE'",
    "WHEN 13 THEN 'TIME'",
    "WHEN 14 THEN 'CHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ') '",
    'WHEN 16 THEN',
      'CASE F.RDB$FIELD_SUB_TYPE',
        "WHEN 0 THEN 'BIGINT'",
        "WHEN 1 THEN 'NUMERIC(' || F.RDB$FIELD_PRECISION || ', ' || (-F.RDB$FIELD_SCALE) || ')'",
       "WHEN 2 THEN 'DECIMAL'",
      'END',
    "WHEN 27 THEN 'DOUBLE'",
    "WHEN 35 THEN 'TIMESTAMP'",
    'WHEN 37 THEN',
     "IIF (COALESCE(f.RDB$COMPUTED_SOURCE,'')<>'',",
      "'COMPUTED BY ' || CAST(f.RDB$COMPUTED_SOURCE AS VARCHAR(250)),",
     "'VARCHAR(' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')')",
    "WHEN 40 THEN 'CSTRING' || (TRUNC(F.RDB$FIELD_LENGTH / CH.RDB$BYTES_PER_CHARACTER)) || ')'",
    "WHEN 45 THEN 'BLOB_ID'",
    "WHEN 261 THEN 'BLOB SUB_TYPE ' || F.RDB$FIELD_SUB_TYPE  ||   IIF (COALESCE(f.RDB$SEGMENT_LENGTH,'')<>'', ' SEGMENT SIZE ' ||f.RDB$SEGMENT_LENGTH ,'')",
    "ELSE 'RDB$FIELD_TYPE: ' || F.RDB$FIELD_TYPE || '?'",
  'END',
  ', RF.RDB$FIELD_SOURCE)',
   'FIELD_TYPE,',
  "IIF(COALESCE(RF.RDB$NULL_FLAG, 0) = 0, NULL, 'NOT NULL') FIELD_NULL,",
  //'CH.RDB$CHARACTER_SET_NAME FIELD_CHARSET,',
  //'DCO.RDB$COLLATION_NAME FIELD_COLLATION,',
  'CAST(COALESCE(RF.RDB$DEFAULT_SOURCE, F.RDB$DEFAULT_SOURCE) AS VARCHAR(256)) FIELD_DEFAULT,',
  //'F.RDB$VALIDATION_SOURCE FIELD_CHECK,',
 ' RF.RDB$DESCRIPTION FIELD_DESCRIPTION',
'FROM RDB$RELATION_FIELDS RF',
'JOIN RDB$FIELDS F ON (F.RDB$FIELD_NAME = RF.RDB$FIELD_SOURCE)',
'LEFT OUTER JOIN RDB$CHARACTER_SETS CH ON (CH.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID)',
'LEFT OUTER JOIN RDB$COLLATIONS DCO ON ((DCO.RDB$COLLATION_ID = F.RDB$COLLATION_ID) AND (DCO.RDB$CHARACTER_SET_ID = F.RDB$CHARACTER_SET_ID))',
'WHERE (COALESCE(RF.RDB$SYSTEM_FLAG, 0) = 0) AND RF.RDB$UPDATE_FLAG=1',
" and    RF.RDB$RELATION_NAME= '" + (_lowercase_keys?tableName.toUpperCase():tableName) + "'",
'ORDER BY  RF.RDB$RELATION_NAME,RF.RDB$FIELD_POSITION'
  ]

  
  this.db.execute(sql, function(err, columns) {
    if (err) return cb(err)
    cb(null, columns)
  })
}


firebird.getPrimaryKeyDefinition = function(tableName, cb) {
  var opts = this.db._opts;
  var _lowercase_keys = (opts.lowercase_keys?opts.lowercase_keys:false);
  var sql = [
    'SELECT ',
    (_lowercase_keys?' LOWER(RDB$FIELD_NAME) ':'RDB$FIELD_NAME '),
    //' RDB$FIELD_NAME FIELD_NAME',
' AS FIELD_NAME FROM',
  'RDB$RELATION_CONSTRAINTS C,',
  'RDB$INDEX_SEGMENTS S',
'WHERE',
  "C.RDB$RELATION_NAME = '" + (_lowercase_keys?tableName.toUpperCase():tableName) + "' AND",
  "C.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY' AND",
  'S.RDB$INDEX_NAME = C.RDB$INDEX_NAME',
'ORDER BY RDB$FIELD_POSITION'
    
  ]
  this.db.execute(sql, function(err, rs) {
    if (err) return cb(err)
    if (!rs[0]) return cb()
    var pk = []
    rs.forEach(function(r) {
      pk.push(r.field_name)
    })
    cb(null, pk)
  })
}


firebird.upsert = function(client, opts, cb) {
  var _opts = this.db._opts;
  var _lowercase_keys = (_opts.lowercase_keys?_opts.lowercase_keys:false);
  var _tableName = (_lowercase_keys?opts.table.toUpperCase():opts.table) ;
  var self = this

  if (!opts.pkWhere) //insert
  {
    
    var sql = ' select GEN_ID(GEN_'+_tableName+'_ID, 1) AS '+pkField+' from rdb$database';
    var _id ='';
     self._execute(client, sql, function(err, rs) {
            if (err) return cb(err)
            //where com ultimo id
            _id =  rs[0][pkField]












            
          })
    
    //let  _gen=   (_opts.version<3)?'gen_id(GEN_'+_tableName.toUpperCase()+'_ID, '+opts.fbAutoIncrement+'),':'';
    //let  _gen=   (_opts.version<3)?'gen_id(GEN_'+_tableName.toUpperCase()+'_ID, '+opts.fbAutoIncrement+'),':'';
    var sql = [
      //'INSERT INTO ' + opts.table ,
      'INSERT INTO ' + _tableName ,
      '('+((_opts.version<3)?'ID, ':'') + opts.properties.join(', ') + ')',
      'VALUES',
      '('+_id+':' + opts.properties.join(', :') + ')'
      , (_opts.returning)?'RETURNING ID':''
    ].join('\n');

    sql = self.db._query.queryValues(sql, opts.data);
    //console.log(sql );
    
    self.execute(client, sql, function(err, rs) {
      if (err) return cb(err);
      //where com ultimo id
      if (_opts.returning)  { 
        var pkField =Object.keys(rs)[0] ;
        var pkWhere = opts.table + '.' + pkField+ ' = ' + rs[pkField]
        console.log(pkWhere);
        getRow(pkWhere) //ultimo registro 

        return 

        function getRow(pkWhere) {
          var sql = [
            "SELECT *",
            //"FROM " + opts.table ,
            "FROM " + _tableName ,
            "WHERE " + pkWhere
          ].join('\n')
          self.execute(client, sql, function(err, rs) {
            if (err) return cb(err)
            cb(null, rs[0])
          })
        }
      } 
//LAST LINES ERROR TO GENERATOR FIREBIRD TO AUTO INCREMENT
      // get the row that was just inserted
      var where = {};
      var pkField;
      //self.getPrimaryKeyDefinition(opts.table, function(err, pk) {
      self.getPrimaryKeyDefinition(opts.table, function(err, pk) {
        if (err) return cb(err)
        // determine if we inserted null into the primary key
        // TODO: support more than 1 autoincrement field? probably not.
        pk.every(function(key) {
          if (!opts.data[key]) {
            pkField = key
            return false
          }
          where[key] = opts.data[key]
          return true
        })

        if (pkField) {
          //var sql = 'SELECT LAST_INSERT_ID() as ' + pkField
          //var sql = ' select GEN_'+opts.table+'id(GENERATOR_NAME, 0) from rdb$database';
          var sql = ' select GEN_ID(GEN_'+_tableName+'_ID, 0) AS '+pkField+' from rdb$database';
         // var sql = ' select MAX(ID) AS '+pkField+' from '+_tableName;
          self.execute(client, sql, function(err, rs) {
            if (err) return cb(err)
            //where com ultimo id
            //var pkWhere = opts.table + '.' + pkField + ' = ' + rs[0][pkField]
            var pkWhere = opts.table + '.' + pkField + ' = ' + rs[0][pkField]
            getRow(pkWhere) //ultimo registro 
          })
          return
        }

        getRow(opts.pkWhere) //ultimo registro 

        function getRow(pkWhere) {
          var sql = [
            "SELECT *",
            //"FROM " + opts.table ,
            "FROM " + _tableName ,
            "WHERE " + pkWhere
          ].join('\n')
          self.execute(client, sql, function(err, rs) {
            if (err) return cb(err)
            cb(null, rs[0])
          })
        }
      })
    })
  

  }
  else //upsate
  {
    var sql = [
      //'UPDATE ' +  (_lowercase_keys?opts.table.toUpperCase():opts.table)  ,
      'UPDATE ' +  opts.table  ,
      'SET ' + opts.set.join(',\n'),
      'WHERE ' + opts.pkWhere
      //'RETURNING ROW_COUNT '
    ].join('\n');
    
    sql = self.db._query.queryValues(sql, opts.data)
    
    self.execute(client, sql, function(err, rs) {
      if (err) return cb(err)
     // if (rs.affectedRows === 1) {
        var sql = [
          'SELECT *',
          //'FROM ' + opts.table ,
          'FROM ' + _tableName ,
          
          'WHERE ' + opts.pkWhere
        ].join('\n');

        self.execute(client, sql, function(err, rs) {
          if (err) return cb(err)
          cb(null, rs[0])
        })
      

    })
  }
}

//
// Transactions
//

firebird.beginTransaction = function(client, callback) {
  
  //this.execute(client, 'BEGIN', cb)
  return client.transaction(FIREBIRD.ISOLATION_READ_COMMITED, callback);
  
}

firebird.commitTransaction = function(client, cb) {
  //this.execute(client, 'COMMIT', cb)
  client._currentTransaction.commit(function(err) {
    if (err)
      client._currentTransaction.rollback();
    else
      client.detach();
    
    client._currentTransaction = null; //TODO firebird      
    cb(err);
});

}

firebird.rollbackTransaction = function(client, cb) {
  //this.execute(client, 'ROLLBACK', cb)
  client._currentTransaction.rollback(function(err) {
    client._currentTransaction = null; //TODO firebird
    cb(err);
  });
}
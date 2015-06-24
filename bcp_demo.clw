   program


! some equates
bcp_Fail     equate(0)
bcp_Success  equate(1)

H_ENV         equate(long)
H_DBC         equate(long)

sqlSmallint  equate(short)
sqlUSmallInt equate(ushort)
SQLUINTEGER  equate(ulong)

! a structure that is used to store a date
! this maps to the date type in sql server
! BCP operations are designed for speed and very little data conversion is done.
! as a general rule use data types that can be mapped to the server type.
dateStruct group,type
year         sqlSmallInt
mon          sqlUSmallint
day          sqlUSmallint
          end

dateTimeString equate(cstring)
timeString     equate(cstring)


! a queue used as the data source for the bulk insert
! pretty much any data source you want can be used, a tps file, a table from some other
! database, a text file, ...
! there must be one local variable for each table column, except identity columns,
! the local variable is bound to a table column using the ordinal position of the columns
bcpQueue queue,type
fixedString   string(30)
varString     cstring(500)
idNumber      long
realValue     real
dateVar       like(dateStruct)
boolVar       bool
tinyInt       byte
smallInt      short
smallFloat    sreal
datetime      datetimestring(35) !like(dateTimeStruct)
timevalue     timeString(30)
        end

   map
     ! clarion functions to do some work
     ! fill the queue with some random data
     fillQueue(bcpQueue q)
     ! fill the small fixed string with some characters
     fillSmallStr(),string
     ! fill the larger variable string with some characters
     fillLargeStr(),string
     ! worker function to do the binds for each of the queue fields
     BindColumns(bcpQueue q, long h),bool
     module('bcp')
       ! called once t odo some set up, returns an env. handle store the handle locally
       ClaBcpInit(),long,c
       ! called once to establish a connection to the database
       ! and set some connection attributes for BCP
       ! returns a connection hanlde that is used bythe BCP API calls, store locally.
       claBcpConnect(H_ENV hEnv, *cstring connStr),long,c,raw
       ! shuts down the connection and closes the two handles
       ClaBcpKill(H_ENV hEnv, H_DBC hDbc),LONG,PROC,c
       ! sets up the table input for the insert
       ! use the tablename like this, schemaName.TableNAme
       init_Bcp(H_DBC hDbc, *cstring tName),long,c,raw
       ! sends a row
       sendRow_Bcp(H_DBC hDbc),long,c
       ! commit a batch of rows to the database
       batch_Bcp(H_DBC hDbc),long,c
       ! commit all rows to the database and does some clean up on the server.
       ! this function must be called when the process is complete.
       done_Bcp(H_DBC hDbc),long,c
       ! bind a long variable to a table column.
       ! input the connection handle, the local variable and the ordinal position of the
       ! column in the table.  this is one based not zero based.
       bindLong(H_DBC hDbc, *long colv, long colOrd),long,c,name('bind_Bcpl')
       ! bind a real variable
       bindReal(H_DBC hDbc, *real colv, long colOrd),long,c,name('bind_bcpf')
       ! bind a string variable.  note sLen parameter. this should be the size of the
       ! clarion string, use size(string) do not use len(clip(string))
       ! internally, in the C dll, each bind call sends the server the size of the data type
       ! strings can vary in size so the extra parameter is needed.  for most
       ! of the data types the size is known.
       ! use this for the char(x) columns
       bindString(H_DBC hDbc, *string colv, long colOrd, long slen),long,c,raw,name('bind_bcps')
       ! bind a clarion cstring.  Note the size parameter is not used for the cstring.
       ! the system will find the length to insert.
       ! use this for the varchar(x) columns
       bindCStr(H_DBC hDbc, *cstring colv, long colOrd),long,c,raw,name('bind_bcpcs')
       ! bind a boolean variable
       bindBool(H_DBC hDbc, *bool colv, long colOrd),long,c,name('bind_bcpb')
       ! bind a date variable
       bindDate(H_DBC hDbc, *dateStruct colv, long colOrd),long,c,raw,name('bind_bcpd')
       ! bind a date time variable,
       ! used a string in the standard ODBC format for date and times
       ! be sure the dates are formatted correctly, yyyy-mm-dd, use leading zeros
       bindDateTime(H_DBC hDbc, *dateTimeString colv, long colOrd),long,c,raw,name('bind_bcpdt')
       ! bind a byte variable
       bindByte(H_DBC hDbc, *byte colv, long colOrd),long,c,name('bind_Bcpby')
       ! bind a short variable
       bindshort(H_DBC hDbc, *short colv, long colOrd),long,c,name('bind_BcpSh')
       ! bind a sreal variable
       bindSReal(H_DBC hDbc, *sreal colv, long colOrd),long,c,name('bind_Bcpsf')
       ! used a cstring for the time.  watch the time(n) closely
       ! if it over flows the row will not be inserted
       ! be sure the times are formatted correctly, hh:mm:ss.fraction, use leading zeros
       bindTime(H_DBC hDbc, *timeString t, long colOrd),long,c,raw,name('bind_Bcpt')
     end
   end

! the local queue used as the data source
bcpQ  &bcpQueue

! the two handles used by the API
hEnv      H_ENV
hdBC      H_DBC

! how many rows do you want to insert
numberInsert equate(100000)

! local loop counter
loopCnt      long


! if using sql auth add the uid and password values to the connection string
cs           cstring('Driver={{SQL Server Native Client 10.0};Server=server name here;Database=database name here;trusted_connection=yes;')
tableName    cstring(schemaName.tablename')

! number of rows written to the database.
rowsSent  long

result    bool

t         long

  code

  ! allocate and fill the queue
  bcpQ &= new(bcpQueue)
  fillQueue(bcpq)

  ! call the init to do the set up and get the handle back
  hEnv = ClaBcpInit()
  if (hEnv <= 0)
    message('Unable to allocate the env. handle')
    return
  end

  ! now get the connection to the database
  ! this call also sets the connection attributes to allow
  ! bulk operations on the connection
  hDbc = claBcpConnect(hEnv, cs)

  if (hDbc <= 0)
    message('Unable to Connect to the database')
    return
  end

  ! set up the table that will be used
  if (init_Bcp(hDbc, tableName) = BCP_FAIL)
    message('Unable to set up the table for inserts.')
    ClaBcpKill(hEnv, hDbc)
    return
  end

  if (bindColumns(bcpq, hDbc) = false)
    message('Bind failed for one or more columns.')
    ClaBcpKill(hEnv, hDbc)
    return
  end

  t = clock()
  ! iterate over the queue and sed the data to the server.
  loop loopCnt = 1 to records(bcpq)
    get(bcpq, loopCnt)
    if (sendRow_bcp(hDbc) = bcP_Fail)
      message('send row failed')
      ClaBcpKill(hEnv, hDbc)
      return
    end
  end

  rowsSent = done_bcp(hDbc)

  message(format(clock() - t, @t4) & ' to send and write ' & rowsSent & ' rows to the database.')

  ! close trhe connection and free the env handle
  ClaBcpKill(hEnv, hDbc)

  return
! ------------------------------------------------------------------------------------

! bind the queue fields to table columns.
! note the idNumber column is the first field in the queue
! but is the second column in the table.  there is an identity
! column in the table and it is the first column.
! the identity column can be at any ordinal position but the
! binding must be adjusted so there is not data inserted into that column.
BindColumns procedure(bcpQueue q, long h) !,bool

retv   bool

  code

  retv = true

  if (bindLong(h, q.idNumber, 2) = bcp_Fail)
    retv = false
  end

  if (bindReal(h, q.realvalue, 3) = bcp_fail)
    retv = false
  end

  if (bindString(h, q.fixedString, 4, 30) = bcp_fail)
    retv = false
  end

  if (bindCStr(h, q.varString, 5) = bcp_fail)
    retv = false
  end

  if (bindDate(h, q.datevar, 6) = bcp_fail)
    retv = false
  end

  if (bindBool(h, q.boolvar, 7) = bcp_fail)
    retv = false
  end

  if (bindByte(h, q.tinyInt, 8) = bcp_fail)
    retv = false
  end

  if (bindSReal(h, q.smallFloat, 9) = bcp_fail)
    retv = false
  end

  if (bindShort(h, q.smallInt, 10) = bcp_fail)
    retv = false
  end

  if (bindDateTime(h, q.dateTime, 11) = bcp_fail)
    retv = false
  end

  if (bindTime(h, q.timevalue, 12) = bcp_fail)
    retv = false
  end

  return true
! -------------------------------------------------------------------------------------

! fll the queue with some values, don't care what they are
! for the demo
fillQueue procedure(bcpQueue q)

x  long(1)

  code

  loop numberInsert times
    q.idNumber = x
    x += 1
    q.fixedString = fillSmallStr()
    q.varString = clip(fillLargeStr())
    q.realValue = random(1, 23000)
    q.dateVar.year = random(1800, 2020)
    q.dateVar.mon = random(1, 12)
    q.datevar.day = random(1, 28)
    q.boolVar = true
    q.tinyInt = random(1, 254)
    q.smallInt= random(1, 32000)
    q.smallFloat = random(1, 10000)
    q.datetime = clip(random(1800, 2020) & '-' & format(random(1, 12), @n02) & '-' & format(random(1, 28), @n02) & ' 01:01:01.045')
    q.timevalue = '12:34:12.132'
    
    add(q)
  end

  
  return

! these two just generate some random string data
fillSmallStr procedure() ! string

x long
l long
s string(30)

  code

  l = random(1, 30)
  loop x = 1 to l
    s[x] = chr(random(65, 127))
  end

  return s

fillLargeStr procedure() ! string

x long
l long
s string(499)

  code

  l = random(1, 499)
  loop x = 1 to l
    s[x] = chr(random(65, 127))
  end

  return s
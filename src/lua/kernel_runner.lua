package.path = package.path .. ";src/lua/?.lua;?.lua"

local ffi = require("ffi")

require("iter")

ffi.cdef[[
  typedef long i64;
  typedef unsigned int u32;
  typedef unsigned short u16;
  typedef unsigned char u8;
  typedef char i8;


  typedef struct Vdbe Vdbe;
  typedef struct BtCursor BtCursor;
  typedef struct KeyInfo KeyInfo;
  typedef struct Mem Mem;

  int sqlite3BtreeFirst(BtCursor *pCur, int *pRes);

  int sqlite3BtreeNext(BtCursor *pCur, int *pRes);

  int sqlite3BtreeKeySize(BtCursor *pCur, long *pSize);
  const char *sqlite3BtreeDataFetch(BtCursor *pCur, unsigned int *pAmt);

  void vdbe_ParseHeader(const char *row, u32 *aOffset, u32 *aType, int column);
  int vdbe_SeekCursor(Vdbe *pVdbe, BtCursor *pTabCur, BtCursor *pIdxCur);

  struct UnpackedRecord {
    KeyInfo *pKeyInfo;  /* Collation and sort-order information */
    Mem *aMem;          /* Values */
    u16 nField;         /* Number of entries in apMem[] */
    i8 default_rc;      /* Comparison result if keys are equal */
    u8 errCode;         /* Error detected by xRecordCompare (CORRUPT or NOMEM) */
    i8 r1;              /* Value to return if (lhs > rhs) */
    i8 r2;              /* Value to return if (rhs < lhs) */
    u8 eqSeen;          /* True if an equality comparison has been seen */
  };
  typedef struct UnpackedRecord UnpackedRecord;

  int vdbe_SeekIndex(BtCursor *pCur, UnpackedRecord *rec, int opcode_seek);

  Mem *vdbeGetMem(Vdbe *v, int k);
  KeyInfo *vdbeGetCursorKeyInfo(Vdbe *v, int k);
  BtCursor *vdbeGetCursor(Vdbe *v, int k);

  typedef struct str_chunk {
    char *ptr;
    int n;
  } str_chunk;

  typedef struct dyn_string {
    char *ptr;
  } dyn_string;

  typedef struct flexistring {
    bool materialized;
    union {
        str_chunk ref; // used if materialized is 0
        dyn_string buf; // used then materialized is 1
    } mem;
  } flexistring;

  flexistring flexistring_create(const char *str);
  flexistring flexistring_materialize(const flexistring *fs);
  void flexistring_free(flexistring *fs);

  int flexistring_cmp_str(const flexistring *fstr1, const char *str2);
  int flexistring_cmp_flexistring(const flexistring *fstr1, const flexistring *fstr2);

  void flexistring_dbg_print(const flexistring *fs);

  typedef flexistring date_t;

  void *memcpy(void *dest, const void *src, size_t n);
  int strcmp(const char *s1, const char *s2);

  // specialized version
  void sqlite3VdbeSerialGetInt(const unsigned char *buf, u32 serial_type, int *place);
  void sqlite3VdbeSerialGetDouble(const unsigned char *buf, u32 serial_type, double *place);
  void sqlite3VdbeSerialGetChunk(const unsigned char *buf, u32 serial_type, str_chunk *chunk);
  void sqlite3VdbeSerialGetFlexiStr(const unsigned char *buf, u32 serial_type, flexistring *fstr);

  void sqlite3VdbeSerialGetDouble(
  const unsigned char *buf,     /* Buffer to deserialize from */
  u32 serial_type,              /* Serial type to deserialize */
  double *place                 /* Place to write value into */
  );

  void sqlite3VdbeSerialGetChunk(
    const unsigned char *buf,     /* Buffer to deserialize from */
    u32 serial_type,              /* Serial type to deserialize */
    str_chunk *chunk
  );

  void ljkWriteDouble(Vdbe *p, int iRes, double val);
  void ljkWriteInt(Vdbe *p, int iRes, int val);
  int ljkWriteStr(Vdbe *p, int iRes, const char *str);
  int ljkWriteFlexiStr(Vdbe *p, int iRes, const flexistring *str);
]]

if os.getenv("JIT_DUMP") then
   jit_dump = require 'jit.dump'
   jit_dump.on()
end

prof = require 'jit.p'
jit = require 'jit'

local C=ffi.C

local t_flexistring = ffi.typeof("flexistring")
local t_flexistring_ref = ffi.typeof("flexistring &")
local t_flexistring_ptr = ffi.typeof("flexistring *")

function is_flexistring(s)
  return type(s) == 'cdata' and (ffi.typeof(s) == t_flexistring_ref or ffi.typeof(s) == t_flexistring_ptr)
end

function flexistring_create(str)
  return C.flexistring_create(str)
end

function flexistring_materialize(s)
  assert(is_flexistring(s), tostring(ffi.typeof(s)))
  return C.flexistring_materialize(s)
end

function to_lua_string(s)
  assert(is_flexistring(s), 'to_lua_string should be called with flexistring')
  if s.materialized then
    return ffi.string(s.mem.buf.ptr)
  else
    return ffi.string(s.mem.ref.ptr, s.mem.ref.n)
  end
end

ffi.metatype(ffi.typeof("flexistring"), {
               __lt = function(s1,s2)
                 if type(s2) == 'string' then
                   return C.flexistring_cmp_str(s1,s2) < 0
                 elseif type(s1) == 'string' then
                   return C.flexistring_cmp_str(s2,s1) > 0
                 else
                   return C.flexistring_cmp_flexistring(s1,s2) < 0
                 end
               end,
               __le = function(s1,s2)
                 if type(s2) == 'string' then
                   return C.flexistring_cmp_str(s1,s2) <= 0
                 elseif type(s1) == 'string' then
                   return C.flexistring_cmp_str(s2,s1) >= 0
                 else
                   return C.flexistring_cmp_flexistring(s1,s2) <= 0
                 end
               end,
               __eq = function(s1,s2)
                 if type(s2) == 'string' then
                   return C.flexistring_cmp_str(s1,s2) == 0
                 elseif type(s1) == 'string' then
                   return C.flexistring_cmp_str(s2,s1) == 0
                 else
                   return C.flexistring_cmp_flexistring(s1,s2) == 0
                 end
               end,
               __tostring = function(s)
                 return to_lua_string(s)
               end,
               __gc = function(s)
                 C.flexistring_free(s)
               end
})

-- object for filling continious arrays of Mem cells,
-- eg for writing results and doing index seeks
local MemWriter = createClass()
local function createMemWriter(pVdbe, iStart, iSize)
  assert(type(iStart) == 'number')
  assert(type(iSize) == 'number' and iSize > 0)
  return MemWriter:new({pVdbe = pVdbe, iStart = iStart, iPlace = iStart, iSize = iSize})
end

function MemWriter:rewind()
  self.iPlace = self.iStart
end

function MemWriter:incPlace()
  assert(self.iPlace < self.iStart + self.iSize, 'packing more values than place reserved') -- actually we go beyond last element here, but subsequent write will trigger assert
  self.iPlace = self.iPlace+1
end

function MemWriter:putInt(num)
  C.ljkWriteInt(self.pVdbe, self.iPlace, num)
  self:incPlace()
end

function MemWriter:putDouble(num)
  C.ljkWriteDouble(self.pVdbe, self.iPlace, num)
  self:incPlace()
end

function MemWriter:putString(str)
  if type(str) == 'string' then
    C.ljkWriteStr(self.pVdbe, self.iPlace, str)
  elseif ffi.typeof(str) == t_flexistring_ref then
    C.ljkWriteFlexiStr(self.pVdbe, self.iPlace, str)
  else error("unknown arg type to putString")
  end
  self:incPlace()
end

function MemWriter:putDate(str)
  C.ljkWriteFlexiStr(self.pVdbe, self.iPlace, str)
  self:incPlace()
end

function MemWriter:putChar(chr)
  local str = string.char(chr)
  C.ljkWriteStr(self.pVdbe, self.iPlace, str)
  self:incPlace()
end

-- called by vdbe on preparation stage before looping
function kernel_entry_point(pVdbe, kernel_id)
   jit.flush()
   if os.getenv("JIT_PROFILE") then
      prof.start(os.getenv("JIT_PROFILE"))
   end

   if os.getenv("VERBOSE_INJECT") then
     io.stderr:write('KERNEL STARTED\n')
   end

   dbg("hello from kernel_entry_point")
   local ctx = assert(g_ctxs[kernel_id], "kernel context found")
   local K = assert(ctx.K, 'kernel found')

   -- opening table iterators
   local iters = {}
   for iter_id, cur in pairs(ctx.cursors) do
     if cur.iIdxCsr then
       dbg("creating index iterator %s", iter_id)
       local pCrsr = C.vdbeGetCursor(pVdbe, assert(cur.iCsr))
       local table_info = assert(cur.table_info)
       local iter_data = assert(K.input_iterators[iter_id], "table is known to kernel")
       local tbl_iter = createSQLiteTableIter(table_info, pCrsr,
                                              iter_data.unpack_row, iter_data.init_iter_fields)
       local idx_key_writer = createMemWriter(pVdbe, table_info.idxKeyPlace, table_info.maxIdxColumn)
       local upackrec = ffi.new('UnpackedRecord')
       upackrec.pKeyInfo = C.vdbeGetCursorKeyInfo(pVdbe, cur.iIdxCsr)
       upackrec.aMem = C.vdbeGetMem(pVdbe, table_info.idxKeyPlace)
       upackrec.nField = table_info.maxIdxColumn

       local pIdxCrsr = C.vdbeGetCursor(pVdbe, assert(cur.iIdxCsr))
       local idx_iter = createSQLiteTableIter({ bufferCTypeName = table_info.bufferCTypeName,
                                                unpackedRowWriter = idx_key_writer,
                                                unpackedRecord = upackrec,
                                                maxColumn = table_info.maxIdxColumn },
                                              pIdxCrsr, nil, function() end)

       local paired_iter = createSQLitePairedIndexIter(pVdbe, idx_iter, tbl_iter)
       iters[iter_id] = function()
         paired_iter:rewind()
         return paired_iter
       end
     else
       dbg("creating iterator %s", iter_id)
       local pCrsr = C.vdbeGetCursor(pVdbe, cur.iCsr)
       local table_info = assert(cur.table_info)
       local iter_data = assert(K.input_iterators[iter_id], "table is known to kernel")
       local tbl_iter = createSQLiteTableIter(table_info, pCrsr,
                                              iter_data.unpack_row, iter_data.init_iter_fields)
       iters[iter_id] = function()
         tbl_iter:rewind()
         return tbl_iter
       end
     end
   end

   -- preparing result writer object
   local vdbe_resp = createMemWriter(pVdbe, ctx.iRes, table.size(K.result_columns))

   local init_res
   if K.kernel_init then
     dbg("running K.kernel_init")
     init_res = K.kernel_init(ctx.params)
   end
   -- registering result iterator and result writer for futher calls from iterator_entry_point
   dbg("running K.result_iterator")
   ctx.result_iter = K.result_iterator(iters, ctx.params, init_res)
   ctx.result_builder = vdbe_resp

   if os.getenv("JIT_PROFILE") then
      prof.stop()
   end

   return 0
end

-- called by vdbe loop, returns single result row
function iterator_entry_point(pVdbe, kernel_id)
   dbg("hello from iterator_entry_point")

   if os.getenv("JIT_PROFILE") then
      prof.start(os.getenv("JIT_PROFILE"))
   end

   local ctx = assert(g_ctxs[kernel_id], "kernel context found")
   local K = assert(ctx.K, 'kernel found')

   local res = ctx.result_iter:next()

   if res == nil then
      C.ljkWriteInt(pVdbe, ctx.iStopFlag, 1);
   else
     ctx.result_builder:rewind()
     K.packResponse(ctx.result_builder, res)
   end

   if os.getenv("JIT_PROFILE") then
      prof.stop()
   end

   return 0
end

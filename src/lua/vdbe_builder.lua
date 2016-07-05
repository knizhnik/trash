local ffi = require 'ffi'

ffi.cdef [[
typedef struct Parse Parse;
typedef struct Table Table;
typedef struct Index Index;

void ljGenReadLock(Parse *pParse, Table *pTab);
int ljGenCursorOpen(Parse *pParse, Table *pTab, int maxCol);
void ljGenCursorClose(Parse *pParse, int iCsr);
int ljGenIndexOpen(Parse *pParse, Table *pTab, Index *pBest);

void ljGenKernelCall(Parse *pParse, const char *kernel_name, int arg1, int arg2, int arg3);

int ljReserveMem(Parse *pParse, int n_cells);
void ljGenResultRow(Parse *pParse, int place, int n_results);

void ljGenInteger(Parse *pParse, int cell, int val);

int ljMakeLabel(Parse *pParse);
void ljResolveLabel(Parse *pParse, int x);

void ljGenGoto(Parse *pParse, int label);
void ljGenGotoIfPos(Parse *pParse, int cell, int label, int decr);

]]

local C = ffi.C

function dbg(...)
   if os.getenv("DEBUG") == '1' then
      print(...)
   end
end

-- Test Vdbe Building
function print_table(t,off)
   off = off or 0
   local offset = string.rep(' ', (off + 1) * 8)
   for i,v in pairs(t) do
     if type(v) == 'table' then
         io.stderr:write(offset, i, ' TABLE:\n')
         print_table(v, off+1)
      else
         io.stderr:write(offset, i, ' ',  tostring(v),'\n')
      end
   end
end

local function find_max_column(columns_data, used_columns)
   assert(columns_data)
   assert(used_columns)
   local max_idx = 0
   for _,c in ipairs(used_columns) do
     local idx = columns_data[c]
     assert(idx, "unknown column: " .. c)
      if idx > max_idx then
         max_idx = idx
      end
   end
   return max_idx
end

-- calculates num entries in hash table or array
function table.size(tbl)
   local s = 0
   for i,v in pairs(tbl) do
      s = s+1
   end
   return s
end

function common_vdbe_builder(db, pParse, kernel_name, kernel_code)
   dbg("hello from common_vdbe_builder")

   g_kernels = g_kernels or {}

   -- TODO fix kernel caching mechanism, now it's disabled
   local kernel_id = 0
   g_kernels[kernel_id] = nil

   local K
   if not g_kernels[kernel_id] then
      dbg("compiling kernel ", kernel_name)
      K = loadstring(kernel_code, "kernel_code")()
      ffi.cdef(K.ffi_decls)
      g_kernels[kernel_id] = K
   else
      K = g_kernels[kernel_id]
   end

   local params = getKernelParameters()
   local cursors = {}
   -- gen opening cursors
   for k_iter_id, k_tbl_data in pairs(K.input_iterators) do
     local t_name = k_tbl_data.table
     assert(t_name, "input iterator should supply table name")
     local table_data = getSqlTableData(db, t_name)
     local pTbl = assert(table_data.pTab , "cant get table ptr")
     local table_indices = getSqlTableIndices(pTbl)

     C.ljGenReadLock(pParse, pTbl)

     if not k_tbl_data.index then -- prepare to open ordinal cursor
       local max_idx = find_max_column(table_data.columns, k_tbl_data.accessed_columns)
       local iCsr = C.ljGenCursorOpen(pParse, pTbl, max_idx)
       local tbl_info = {
         maxColumn = max_idx,
         bufferCTypeName = k_tbl_data.buffer_ctype,
         getColumnId = function(self, c) return table_data.columns[c] end
       }
       cursors[k_iter_id] = { table_info = tbl_info, iCsr = iCsr }
     else -- prepare to open index cursor
       local idx = assert(table_indices[k_tbl_data.index], "index not found: " .. k_tbl_data.index)
       local iIdxCsr = C.ljGenIndexOpen(pParse, pTbl, idx.ptr)
       local idx_key_place = C.ljReserveMem(pParse, #idx.columns) -- space for UnpackedRow needed for search
       local max_idx = find_max_column(table_data.columns, k_tbl_data.accessed_columns)
       local iCsr = C.ljGenCursorOpen(pParse, pTbl, max_idx)

       local table_info = {
         maxColumn = max_idx,
         maxIdxColumn = #idx.columns,
         idxKeyPlace = idx_key_place,
         bufferCTypeName = k_tbl_data.buffer_ctype,
         getColumnId = function(self, c) return table_data.columns[c] end
       }

       cursors[k_iter_id] = { table_info = table_info, iCsr = iCsr, iIdxCsr = iIdxCsr }
     end

   end

   C.ljGenKernelCall(pParse, "kernel_entry_point", kernel_id,0,0)
   -- gen place for results
   local n_res = table.size(K.result_columns)
   local res_place = C.ljReserveMem(pParse, n_res)
   -- gen iterator calling loop
   local stop_flag = C.ljReserveMem(pParse, 1)
   local loop_lbl = C.ljMakeLabel(pParse)
   local stop_lbl = C.ljMakeLabel(pParse)

   C.ljGenInteger(pParse, stop_flag, 0)
   C.ljResolveLabel(pParse, loop_lbl)
   C.ljGenKernelCall(pParse, "iterator_entry_point", kernel_id,0,0)
   C.ljGenGotoIfPos(pParse, stop_flag, stop_lbl, 0)
   C.ljGenResultRow(pParse, res_place, n_res)
   C.ljGenGoto(pParse, loop_lbl)
   C.ljResolveLabel(pParse, stop_lbl)
   for _,cur_data in pairs(cursors) do
      C.ljGenCursorClose(pParse, cur_data.iCsr)
   end

   -- storing context
   g_ctxs = g_ctxs or {}
   g_ctxs[kernel_id] = {
      K = K,
      cursors = cursors,
      iStopFlag = stop_flag,
      iRes = res_place,
      params = params
   }

end

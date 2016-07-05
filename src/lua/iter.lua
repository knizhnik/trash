package.path = package.path .. ";./?.lua"
require("oop")

local ffi = require 'ffi'
local C = ffi.C

ffi.cdef [[
  enum SeekOps { OP_SeekLT = 64, OP_SeekLE = 65, OP_SeekGE = 66, OP_SeekGT = 67};
]]

Iter = createClass()
function Iter:seekIndex(...)
  if self.source then return self.source:seekIndex(...)
  else error('seekIndex on non-index kernel')
  end
end

function Iter:rewind(...)
  if self.source then return self.source:rewind(...)
  else error('no source to pass call further')
  end
end

function Iter:isEmpty()
  if self.source then
    res = self.source:next()
    return res == nil
  else
    error "no source"
  end
end

function Iter:next() error"NYI: Iter.next" end

-- Print
local PrintIter = Iter:new( {source = {} })
function PrintIter:next()
  local v = self.source:next()
  print(self.caption, v)
  return v
end

function Iter:print(caption)
  return PrintIter:new({source = self, caption = caption})
end

-- Count
local CountIter = Iter:new( {source = {} })
function CountIter:next()
  local v = self.source:next()
  if v == nil then
    print(self.label, self.cnt)
  end
  self.cnt = self.cnt + 1

  return v
end

function Iter:count(lbl)
  return PrintIter:new({source = self, label = lbl, cnt = 0})
end

-- Map
local MapIter = Iter:new({ source = {}; mapFunc = nil; })
function Iter:map(f)
    return MapIter:new({ source = self, mapFunc = f })
end

function MapIter:next()
  local elem = self.source:next()
  return elem and self.mapFunc(elem)
end

-- MapU
local MapUIter = Iter:new({ source = {}; mapFunc = nil; })
function Iter:mapU(f, state)
    return MapUIter:new({ source = self, mapFunc = f, state = state })
end

function MapUIter:next()
    local elem = self.source:next()
    if elem then
      self.mapFunc(self.state, elem)
      return self.state
    end
    return nil
end

-- FlatMap
local FlatMapIter = Iter:new({ source = {}; mapFunc = nil; inner = nil })
function Iter:flatMap(f)
    return FlatMapIter:new({ source = self, mapFunc = f })
end

function FlatMapIter:next()
  local elem
  repeat
      if not self.inner then
          local source = self.source:next()
          if source == nil then return nil end
          self.inner = self.mapFunc(source)
      end
      elem = self.inner:next()
      if elem == nil then
          self.inner = nil
      end
  until not (elem == nil)
  return elem
end

-- Filter
local FilterIter = Iter:new({ source = {}; predicateFunc = nil; })
function Iter:filter(p)
    return FilterIter:new({ source = self; predicateFunc = p})
end

function FilterIter:next()
  local elem = self.source:next()
  while elem and not self.predicateFunc(elem) do
    elem = self.source:next()
  end
  return elem
end

-- RangeFilter
local CmpFilterIter = Iter:new()
function Iter:moreEqFilter(val)
  return CmpFilterIter:new({source = self, bound = val, seek_op = C.OP_SeekGE})
end

function CmpFilterIter:next()
  if self.seek_op then
    assert(type(self.bound) == 'number', 'can only do index-filtering on numbers these days')
    self.source.idx_iter.unpackedRowWriter:rewind()
    self.source.idx_iter.unpackedRowWriter:putDouble(self.bound)
    local res = C.vdbe_SeekIndex(self.source.idx_iter.pCrsr, self.source.idx_iter.unpackedRecord, self.seek_op)
    self.seek_op = nil -- single seek is enough
    if res > 0 then return nil end
  end

  return self.source:next()
end

-- Reduce
local ReduceIter = Iter:new()
function Iter:reduce(f, init)
   return ReduceIter:new({ source = self; reduceFunc = f; val0 = init() })
end

function ReduceIter:next()
   local val = self.val0
   self.val0 = nil
   while true do
      local elem = self.source:next()
      if elem == nil then break end
      val = self.reduceFunc(val, elem)
   end
   return val
end

-- ReduceU
local ReduceIterU = Iter:new()
function Iter:reduceU(f, init)
  if type(init) == 'function' then
    init = init()
  end
  return ReduceIterU:new({ source = self; reduceProc = f; val = init; done=false })
end

function ReduceIterU:next()
   if self.done then return nil end

   while true do
      local elem = self.source:next()
      if elem == nil then break end
      self.reduceProc(self.val, elem)
   end
   self.done = true
   return self.val
end

-- HashJoin
local HashJoinIter = Iter:new()
function Iter:join(inner_iter, outer_key, inner_key, clone_inner_elt)
  local inner_tbl = {}
  local in_elt = inner_iter:next()

  repeat
    if in_elt == nil then break end
    local in_key = inner_key(in_elt)
    local val = inner_tbl[in_key]
    local elt_dup = clone_inner_elt(in_elt)

--    if val then -- need to store all variants
      if (type(val) == "table") then
        table.insert(val, elt_dup)
      else
        inner_tbl[in_key] = { elt_dup } -- {val, in_copy}
      end
--    else
--      inner_tbl[in_key] = in_copy
--    end

    in_elt = inner_iter:next()
  until not in_elt

  return HashJoinIter:new({ outer_iter = self, outer_key = outer_key, inner_tbl = inner_tbl})
end


function HashJoinIter:next()
  local elem
  repeat
    if not self.in_values then
      self.out_elt = self.outer_iter:next()
      if self.out_elt == nil then
        inner_tbl = nil
        return nil
      end

      local out_k = self.outer_key(self.out_elt)
      self.in_values = self.inner_tbl[out_k]

      if self.in_values then
        self.in_idx = #self.in_values
      end
    end
    local in_val = self.in_values
--    print("iv", in_val, self.in_idx, self.out_elt)
    if type(in_val) == 'table' then
--      print("tbl", #in_val)
--      print_table(in_val)

      elem = { self.out_elt, in_val[self.in_idx] }
      self.in_idx = self.in_idx - 1
--      print("rest", #in_val)
      if self.in_idx == 0 then
--        print("zeroing")
        self.in_values = nil
      end
--    elseif in_val ~= nil then
--      print("ivt", type(in_val))
--      self.in_values = nil
--      elem = { self.out_elt, self.in_val }
    end
  until elem
  return elem
end

--
-- MergeJoin
--
local IdxJoinIter = Iter:new()
function Iter:indexed_join(inner_iter, outer_key, inner_key)
  return IdxJoinIter:new({ outer_iter = self, inner_iter = inner_iter,
                           outer_key = outer_key, inner_key = inner_key })
end

function IdxJoinIter:next()
  while true do
    if not self.o_elt then
      self.o_elt = self.outer_iter:next()
      if not self.o_elt then -- outer table finished
        return nil
      end

      self.o_key = self.outer_key(self.o_elt)
      self.inner_iter:seekIndex({ self.o_key }, C.OP_SeekGE)
    end

    local i_elt = self.inner_iter:next()

    if i_elt then
      local i_key = self.inner_key(i_elt)
      assert(type(i_key) == 'number', 'TODO: can only do index-join on numbers')
      if self.o_key < i_key then -- all remaining inner elts are larger
        self.o_elt = nil
      elseif self.o_key == i_key then
        return { self.o_elt, i_elt }
      else
        assert("index LIES")
      end
    else -- no more pairs for outer element
      self.o_elt = nil
    end
  end
end

-- Sort
local ArrayIter = Iter:new({array = {}, i=0})
function createArrayIter(tbl)
  return ArrayIter:new({array = tbl})
end

function ArrayIter:next()
  self.i = self.i + 1

  return self.array[self.i]
end

-- sort by Less-than predicate
function Iter:sort(pred)
  local tbl = {}
  while true do
    local elt = self:next()
    if not elt then break end
    table.insert(tbl, elt)
  end
  table.sort(tbl, pred)
  return createArrayIter(tbl)
end

-- sorting by c-like predicate
function Iter:sortBy(int_pred)
  local pred = function(a,b)
    return int_pred(a,b) < 0
  end

  return self:sort(pred)
end

-- TableIter
TableIter = Iter:new({ nextFunc = nil, sourceTable = {}, currKey = nil })
function TableIter:next()
    local i,v
    i, v = self.nextFunc(self.sourceTable, self.currKey)
    self.currKey = i
    if i == nil then return nil else return {key = i, val = v} end
end

function createTableIter (t)
    local next, ht1, i = pairs(t)
    return TableIter:new({ nextFunc = next, sourceTable = t, currKey = i})
end

-- MapReduce
function Iter:mapReduce(mapKey, packKey, newValue, reduceValue)
  local ht = {}
  local kt = {}
  local elem = self:next()
  while elem do
    local packedKey = packKey(elem)
    local existingV = ht[packedKey]
    if existingV then
      ht[packedKey] = reduceValue(existingV, elem)
    else
      local newV = newValue()
      newV = reduceValue(newV, elem)
      ht[packedKey] = newV
      kt[packedKey] = mapKey(elem)
    end
    elem = self:next()
  end

  local res_t = {}
  for pk,value in pairs(ht) do
    table.insert(res_t, {key = kt[pk], val = value})
  end

  return createArrayIter(res_t)
end

-- MapReduceU
-- (iter: Row*, mapKey: Row => K, mapValue: Row => V, reduceValue: (V, Row) => Unit, packKey: K => String, cloneKey: K => K, newKey: () => K, newValue: () => V) => DF[(K,V)]
function Iter:mapReduceU(mapKey, packKey, newValue, reduceValue)
  local ht = {}
  local kt = {}

  local elem = self:next()
  while elem do
    local packedKey = packKey(elem)
    local existingV = ht[packedKey]
    if existingV then
      reduceValue(existingV, elem)
    else
      local newV = newValue()
      reduceValue(newV, elem)
      ht[packedKey] = newV
      kt[packedKey] = mapKey(elem)
    end
    elem = self:next()
  end

  local res_t = {}
  for pk,value in pairs(ht) do
    table.insert(res_t, {key = kt[pk], val = value})
  end

  return createArrayIter(res_t)
end

-- Array
function Iter:toArray()
  local res = {}
  local i = 1
  local elem = self:next()
  while elem do
    res[i] = elem
    i = i + 1
    elem = self:next()
  end
  return res
end

-- Foreach
function Iter:foreach(action)
  local elem = self:next()
  while elem do
    action(elem)
    elem = self:next()
  end
end

-- Range
local RangeIter = Iter:new({ length = 0, position = -1 })
function Iter.range(n) return RangeIter:new({ length = n }) end

function RangeIter:next()
    self.position = self.position + 1
    return self.position < self.length and self.position or nil
end

-- SQLTable
local SQLiteTableIter = Iter:new({})

function createSQLiteTableIter(tableInfo, pCrsr, rowParser, initFieldPtrs)
  assert(type(pCrsr == 'cdata'))
  local max_column = tableInfo.maxColumn
  local res = SQLiteTableIter:new({
    tableInfo = tableInfo,
    max_column = max_column,
    rowBuffer = ffi.new(tableInfo.bufferCTypeName),  -- e.g. "LineitemProjection"
    pCrsr = pCrsr,
    finish = nil,
    rowParser = rowParser,
    unpackedRowWriter = tableInfo.unpackedRowWriter, -- non-nil for indexes only
    unpackedRecord = tableInfo.unpackedRecord,       -- non-nil for indexes only
    initFieldPtrs = initFieldPtrs,
    res64 = ffi.new("long[1]"),
    res32 = ffi.new("int[1]"),
    aOffset = ffi.new("int[?]", max_column+2), -- +1 because column numbering starts from 1
    aType = ffi.new("int[?]", max_column+2),   -- and +1 because next column's offset is read for last column
    pChunk = ffi.new("str_chunk"),
  })
  res:initFieldPtrs(res.rowBuffer)
  res:rewind()

  return res
end

-- returns current row underlying cursor points at
function SQLiteTableIter:get()
  if self.finish then return nil end

  C.sqlite3BtreeKeySize(self.pCrsr, self.res64)
  local row = C.sqlite3BtreeDataFetch(self.pCrsr, self.res32);
  local row_len = self.res32[0]

  C.vdbe_ParseHeader(row, self.aOffset, self.aType, self.max_column)
  assert(row_len >= self.aOffset[self.max_column+1])

  self:rowParser(row)
  return self.rowBuffer
end

-- sets cursor to point on the first element
function SQLiteTableIter:rewind()
  self.rc = C.sqlite3BtreeFirst(self.pCrsr, self.res32)
  assert(self.rc == 0)

  self.finish = self.res32[0] > 0

  return not self.finish
end

function SQLiteTableIter:isEmpty()
  return self.finish
end

-- moves iter forward, returns true if not at end
function SQLiteTableIter:advance()
  self.res32[0] = 0
  self.rc = C.sqlite3BtreeNext(self.pCrsr, self.res32)
  if self.rc ~= 0 then error("SQLITE_ERROR") end
  self.finish = self.res32[0] ~= 0
  return not self.finish
end

-- returns row and advances cursor
function SQLiteTableIter:next()
  if self.finish then return nil end
  self:get()
  self:advance()
  return self.rowBuffer
end

function SQLiteTableIter:parseInt(row, columnId, pBuf)
  C.sqlite3VdbeSerialGetInt(row + self.aOffset[columnId], self.aType[columnId], pBuf)
end

function SQLiteTableIter:parseDouble(row, columnId, pBuf)
  C.sqlite3VdbeSerialGetDouble(row + self.aOffset[columnId], self.aType[columnId], pBuf)
end

function SQLiteTableIter:parseByteChunk(row, columnId, pBuf, columnName)
  C.sqlite3VdbeSerialGetChunk(row + self.aOffset[columnId], self.aType[columnId], pBuf)
  self.rowBuffer[columnName] = pBuf.ptr[0]
end

function SQLiteTableIter:parseString(row, columnId, pBuf)
  C.sqlite3VdbeSerialGetFlexiStr(row + self.aOffset[columnId], self.aType[columnId], pBuf)
end

function SQLiteTableIter:parseDate(row, columnId, pBuf)
  self:parseString(row, columnId, pBuf)
end

function getFieldPtr(fieldType, fieldName, rowBuffer)
  return ffi.cast(fieldType, ffi.cast("char *", rowBuffer) + assert(ffi.offsetof(rowBuffer, fieldName), "no such field, " .. fieldName))
end

-- IndexWrapper
local SQLitePairedIndexIter = Iter:new({})
function createSQLitePairedIndexIter(pVdbe, idx_iter, tbl_iter)
  return SQLitePairedIndexIter:new({pVdbe = pVdbe, idx_iter = idx_iter, tbl_iter = tbl_iter})
end

function SQLitePairedIndexIter:get()
  return self.tbl_iter.get()
end

function SQLitePairedIndexIter:seekIndex(keys, op)
  self.idx_iter.unpackedRowWriter:rewind()
  for _,k in ipairs(keys) do
    assert(type(k) == 'number')
    self.idx_iter.unpackedRowWriter:putDouble(k)
  end

  local res = C.vdbe_SeekIndex(self.idx_iter.pCrsr, self.idx_iter.unpackedRecord, op)
  assert(res >= 0, 'C.vdbe_SeekIndex failed')
  self.idx_iter.finish = false
end

function SQLitePairedIndexIter:rewind()
  return self.idx_iter:rewind()
end

function SQLitePairedIndexIter:next()
  if self.idx_iter.finish then return nil end

  assert(C.vdbe_SeekCursor(self.pVdbe, self.tbl_iter.pCrsr, self.idx_iter.pCrsr) == 0, "cant seek")
  local pBuf = self.tbl_iter:get()

  self.idx_iter:advance()
  return pBuf
end

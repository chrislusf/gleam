local mp = require "MessagePack"
mp.set_string 'binary'
local line = {"asdf"}
line['good'] = 123
local encoded = mp.pack(line)
local decoded = mp.unpack(encoded)
print(encoded)
print(string.len(encoded))
print(decoded)
print(decoded[1])
print(decoded['1'])
for k, v in pairs(decoded) do
  print(k,v)
end
for k, v in ipairs(decoded) do
  print(k,v)
end

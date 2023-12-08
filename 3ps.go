package main

import (
   "errors"
   "fmt"
   "net"
   "strings"
   "sync"
)

type SQNode struct {
   data string
   next *SQNode
}

type SetNode struct {
   value string
   next  *SetNode
   prev  *SetNode
}

type HNode struct {
   key   string
   value string
   next  *HNode
   prev  *HNode
}

type Stack struct {
   head *SQNode
   mutex sync.Mutex // Добавляем мьютекс для синхронизации доступа
}

type Queue struct {
   head *SQNode
   tail *SQNode
   mutex sync.Mutex // Добавляем мьютекс для синхронизации доступа
}

type Set struct {
   Table [20]*SetNode
   mutex sync.Mutex // Добавляем мьютекс для синхронизации доступа
}

type HashTable struct {
   Table [20]*HNode
   mutex sync.Mutex // Добавляем мьютекс для синхронизации доступа
}

func (stack *Stack) push(val string) {
   stack.mutex.Lock()
   defer stack.mutex.Unlock()
   node := &SQNode{data: val}
   if stack.head == nil {
      stack.head = node
   } else {
      node.next = stack.head
      stack.head = node
   }
   fmt.Printf("%s added to the stack\n", val)
}

func (stack *Stack) pop() (string, error) {
   stack.mutex.Lock()
   defer stack.mutex.Unlock()
   if stack.head == nil {
      return "", errors.New("stack is empty")
   } else {
      val := stack.head.data
      stack.head = stack.head.next
      return val, nil
   }
}

func (queue *Queue) enqueue(val string) {
   queue.mutex.Lock()
   defer queue.mutex.Unlock()
   node := &SQNode{data: val}
   if queue.head == nil {
      queue.head = node
      queue.tail = node
   } else {
      queue.tail.next = node
      queue.tail = node
   }
   fmt.Printf("%s added to the queue\n", val)
}

func (queue *Queue) dequeue() (string, error) {
   queue.mutex.Lock()
   defer queue.mutex.Unlock()
   if queue.head == nil {
      return "", errors.New("queue is empty")
   } else {
      val := queue.head.data
      queue.head = queue.head.next
      if queue.head == nil {
         queue.tail = nil
      }
      return val, nil
   }
}

func (s *Set) Insert(conn net.Conn, value string) {
   s.mutex.Lock()
   defer s.mutex.Unlock()
   newnode := &SetNode{value: value}
   index := hash(value)
   if s.Table[index] == nil {
      s.Table[index] = newnode
      fmt.Printf("Value %s added to the set\n", value)
      conn.Write([]byte(fmt.Sprintf("Value %s added to the set", value)))
      return
   } else {
      curr := s.Table[index]
      for curr != nil {
         if curr.value == value {
            conn.Write([]byte(fmt.Sprintf("such a value already exists")))
            return
         }
         // Обработка коллизий. Метод цепочек
         if curr.next == nil {
            curr.next = newnode
            curr.next.prev = curr
            fmt.Printf("Value %s added to the set\n", value)
            conn.Write([]byte(fmt.Sprintf("Value %s added to the set", value)))
            return
         }
         curr = curr.next
      }
   }
}

func (s *Set) Remove(conn net.Conn, value string) {
   s.mutex.Lock()
   defer s.mutex.Unlock()
   index := hash(value)
   if s.Table[index] == nil {
      conn.Write([]byte(fmt.Sprintf("Value not found")))
      return
   }
   curr := s.Table[index]
   if curr.value == value {
      s.Table[index] = curr.next
   }
   for curr.value != value {
      curr = curr.next

   }
   if curr.prev != nil {
      curr.prev.next = curr.next
   }
   if curr.next != nil {
      curr.next.prev = curr.prev
   }
   fmt.Printf("Value %s removed from the set\n", value)
   conn.Write([]byte(fmt.Sprintf("Value %s removed from the set", value)))
}

func (s *Set) get(conn net.Conn, value string) {
   s.mutex.Lock()
   defer s.mutex.Unlock()
   index := hash(value)
   curr := s.Table[index]
   for curr != nil {
      if curr.value == value {
         conn.Write([]byte(fmt.Sprintf("Value: %s", curr.value)))
         return
      }

      curr = curr.next // указатель на следующий ключ
   }

   conn.Write([]byte(fmt.Sprintf("Value not found")))
}

func hash(key string) int {
   intKey := 0
   for i := 0; i < len(key); i++ {
      intKey += int(key[i])
   }
   return intKey % 20
}

func (hTable *HashTable) insert(conn net.Conn, key string, value string) {
   hTable.mutex.Lock()
   defer hTable.mutex.Unlock()
   newnode := &HNode{key: key, value: value}
   index := hash(key)
   if hTable.Table[index] == nil {
      hTable.Table[index] = newnode
      fmt.Printf("key %s and value %s added to the hashtable\n", key, value)
      conn.Write([]byte(fmt.Sprintf("key %s and value %s added to the hashtable", key, value)))
      return
   } else {
      curr := hTable.Table[index]
      for curr != nil {
         if curr.key == key {
            conn.Write([]byte(fmt.Sprintf("such a key %s already exists", key)))
            return
         }
         // Обработка коллизий. Метод цепочек
         if curr.next == nil {
            curr.next = newnode
            curr.next.prev = curr
            fmt.Printf("key %s and value %s added to the hashtable\n", key, value)
            conn.Write([]byte(fmt.Sprintf("key %s and value %s added to the hashtable", key, value)))
            return
         }
         curr = curr.next
      }
   }
}

func (hTable *HashTable) remove(conn net.Conn, key string) {
   hTable.mutex.Lock()
   defer hTable.mutex.Unlock()
   index := hash(key)
   if hTable.Table[index] == nil {
      conn.Write([]byte(fmt.Sprintf("Key not found")))
      return
   }
   curr := hTable.Table[index]
   if curr.key == key {
      hTable.Table[index] = curr.next
   }
   for curr.key != key {
      curr = curr.next

   }
   if curr.prev != nil {
      curr.prev.next = curr.next
   }
   if curr.next != nil {
      curr.next.prev = curr.prev
   }
   fmt.Printf("Key %s removed from the hashtable\n", curr.key)
   conn.Write([]byte(fmt.Sprintf("Key: %s, Value: %s", curr.key, curr.value)))
}

func (hTable *HashTable) Get(conn net.Conn, key string) {
   hTable.mutex.Lock()
   defer hTable.mutex.Unlock()
   index := hash(key)
   curr := hTable.Table[index]
   for curr != nil {
      if curr.key == key {
         conn.Write([]byte(fmt.Sprintf("Key: %s, Value: %s", curr.key, curr.value)))
         return
      }

      curr = curr.next // указатель на следующий ключ
   }
   conn.Write([]byte(fmt.Sprintf("Key not found")))
}

func handleConnection(conn net.Conn, set *Set, hTable *HashTable, stack *Stack, queue *Queue) {
   defer conn.Close()

   for {
      buffer := make([]byte, 1024)
      n, err := conn.Read(buffer)
      if err != nil {
         fmt.Println("Error reading:", err)
         return
      }

      input := strings.ToLower(strings.TrimSpace(string(buffer[:n])))
      tokens := strings.Split(input, " ")

      switch tokens[0] {
      case "spush":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         value := strings.TrimSpace(tokens[1])
         stack.push(value)
         conn.Write([]byte(fmt.Sprintf("Value %s added to the stack", value)))

      case "spop":
      if len(tokens) < 1 {
         conn.Write([]byte("Invalid command format"))
         continue
      }
      values := make([]string, 0)
      for {
         val, err := stack.pop()
         if err != nil {
            fmt.Println(err)
            break
         }
         fmt.Printf("%s pop from stack\n", val)
         values = append(values, val)
      }
      conn.Write([]byte(fmt.Sprintf("%v", values)))

      case "qpush":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         value := strings.TrimSpace(tokens[1])
         queue.enqueue(value)
         conn.Write([]byte(fmt.Sprintf("Value %s added to the queue", value)))

      case "qpop":
      if len(tokens) < 1 {
         conn.Write([]byte("Invalid command format"))
         continue
      }
      values := make([]string, 0)
      for {
         val, err := queue.dequeue()
         if err != nil {
            fmt.Println(err)
            break
         }
         fmt.Printf("%s pop from queue\n", val)
         values = append(values, val)
      }
      conn.Write([]byte(fmt.Sprintf("%v", values)))
      
      case "sadd":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         value := strings.TrimSpace(tokens[1])
         set.Insert(conn, value)
      case "srem":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         value := strings.TrimSpace(tokens[1])
         set.Remove(conn, value)
      
      case "sismembers":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         value := strings.TrimSpace(tokens[1])
         set.get(conn, value)

      case "hset":
         if len(tokens) < 3 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         value := strings.TrimSpace(tokens[2])
         key := strings.TrimSpace(tokens[1])
         hTable.insert(conn, key, value)

      case "hget":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         key := strings.TrimSpace(tokens[1])
         hTable.Get(conn, key)
      case "hdel":
         if len(tokens) < 2 {
            conn.Write([]byte("Invalid command format"))
            continue
         }
         key := strings.TrimSpace(tokens[1])
         hTable.remove(conn, key)
      default:
         conn.Write([]byte("Invalid command"))
      }
   }
}

func main() {
   set := &Set{}
   hTable := &HashTable{}
   stack := &Stack{}
   queue := &Queue{}

   ln, err := net.Listen("tcp", ":6379")
   if err != nil {
      fmt.Println("Error starting server:", err)
      return
   }
   defer ln.Close()

   for {
      conn, err := ln.Accept()
      if err != nil {
         fmt.Println("Error accepting connection:", err)
         return
      }

      go handleConnection(conn, set, hTable, stack, queue)
   }
}
#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"
#include <stack>
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  // Get the value associated with the given key.
  // 1. If the key is not in the trie, return nullptr.
  // 2. If the key is in the trie but the type is mismatched, return nullptr.
  // 3. Otherwise, return the value.
  //取root节点
  if(root_ == nullptr){
    return nullptr;
  }
  std::shared_ptr<const TrieNode> node = root_;
  for(auto c : key){
    if(node->children_.find(c) == node->children_.end()){
      return nullptr;
    }
    node = node->children_.at(c);
  }
  if(node->is_value_node_){
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
    if(value_node == nullptr){
      return nullptr;
    }
    return value_node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //throw NotImplementedException("Trie::Put is not implemented.");
  std::shared_ptr<const TrieNode> current = this->root_;
  std::unique_ptr<TrieNode> newRoot = (root_ != nullptr) ? root_->Clone() : std::make_unique<TrieNode>();
  std::shared_ptr<TrieNode> node = std::shared_ptr<TrieNode>(std::move(newRoot));
  current = node;
  std::shared_ptr<TrieNode> parent = nullptr;
  for(auto c : key){
    //不存在该节点
    if(node->children_.find(c) == node->children_.end()){
      std::shared_ptr<TrieNode> newNode = std::make_shared<TrieNode>();
      node->children_.insert({c, newNode});
      parent = node;
      node = newNode;
    } else{
      parent = node;
      node = node->children_[c]->Clone();
      parent->children_[c] = node;
    }
  }
  char last = key[key.size()-1];
  std::shared_ptr<T> val = std::make_shared<T>(std::move(value));
  TrieNodeWithValue node_with_val(node->children_, val);
  parent->children_[last] = std::make_shared<const TrieNodeWithValue<T>>(node_with_val);
  return Trie(std::move(current));
}

auto Trie::Remove(std::string_view key) const -> Trie {
//  //throw NotImplementedException("Trie::Remove is not implemented.");
if (root_ == nullptr || key.empty()) {
  return *this;
}

// Clone the path from root to the key node
std::shared_ptr<TrieNode> newRoot = std::const_pointer_cast<TrieNode>(std::shared_ptr(root_->Clone()));
std::shared_ptr<TrieNode> node = newRoot;
std::shared_ptr<TrieNode> parent = nullptr;
char lastChar;

for (char c : key) {
  if (node->children_.find(c) == node->children_.end()) {
    // Key not found, return original trie
    return *this;
  }
  parent = node;
  lastChar = c;
  node = std::const_pointer_cast<TrieNode>(std::shared_ptr(node->children_.at(c)->Clone()));
  parent->children_[c] = node;
}

// Now, node points to the key node and parent to its parent
if (!node->is_value_node_) {
  // Key node not a value node, return original trie
  return *this;
}

// Remove the key node
parent->children_.erase(lastChar);

// Remove empty nodes up the tree
while (parent != newRoot && parent->children_.empty() && !parent->is_value_node_) {
  char charToParent = key[key.find_last_of(lastChar) - 1];
  node = parent;
  // Find the parent of the current node
  std::shared_ptr<TrieNode> tempParent = newRoot;
  for (char c : key.substr(0, key.find_last_of(lastChar))) {
    tempParent = std::const_pointer_cast<TrieNode>(tempParent->children_.at(c));
  }
  parent = tempParent;
  parent->children_.erase(charToParent);
  lastChar = charToParent;
}
return Trie(std::const_pointer_cast<const TrieNode>(newRoot));
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub

#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"
namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  // throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  // Get the value associated with the given key.
  // 1. If the key is not in the trie, return nullptr.
  // 2. If the key is in the trie but the type is mismatched, return nullptr.
  // 3. Otherwise, return the value.
  //取root节点
  if (root_ == nullptr) {
    return nullptr;
  }
  if (key.empty()) {
    if (root_->children_.find('\0') == root_->children_.end()) {
      return nullptr;
    }
    auto node = root_->children_.at('\0');
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
    return value_node->value_.get();
  }
  std::shared_ptr<const TrieNode> node = root_;
  for (auto c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      return nullptr;
    }
    node = node->children_.at(c);
  }
  if (node->is_value_node_) {
    auto value_node = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
    if (value_node == nullptr) {
      return nullptr;
    }
    return value_node->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // throw NotImplementedException("Trie::Put is not implemented.");
  if (key.empty()) {
    auto new_root = std::make_shared<TrieNode>(root_->children_);
    auto val = std::make_shared<T>(std::move(value));
    auto value_node = std::make_shared<const TrieNodeWithValue<T>>(val);
    new_root->children_['\0'] = value_node;
    return Trie(new_root);
  }
  std::shared_ptr<const TrieNode> current = this->root_;
  std::unique_ptr<TrieNode> new_root = (root_ != nullptr) ? root_->Clone() : std::make_unique<TrieNode>();
  std::shared_ptr<TrieNode> node = std::shared_ptr<TrieNode>(std::move(new_root));
  current = node;
  std::shared_ptr<TrieNode> parent = nullptr;
  for (auto c : key) {
    //不存在该节点
    if (node->children_.find(c) == node->children_.end()) {
      std::shared_ptr<TrieNode> new_node = std::make_shared<TrieNode>();
      node->children_.insert({c, new_node});
      parent = node;
      node = new_node;
    } else {
      parent = node;
      node = node->children_[c]->Clone();
      parent->children_[c] = node;
    }
  }
  char last = key[key.size() - 1];
  std::shared_ptr<T> val = std::make_shared<T>(std::move(value));
  TrieNodeWithValue node_with_val(node->children_, val);
  parent->children_[last] = std::make_shared<const TrieNodeWithValue<T>>(node_with_val);
  return Trie(std::move(current));
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  //throw NotImplementedException("Trie::Remove is not implemented.");
  if (root_ == nullptr) {
    return *this;
  }
  if (key.empty()) {
    std::shared_ptr<TrieNode> new_root = std::shared_ptr(root_->Clone());
    if (new_root->children_.find('\0') != new_root->children_.end()) {
      new_root->children_.erase('\0');
    }
    return Trie(new_root);
  }

  // Clone the path from root to the key node
  std::shared_ptr<TrieNode> new_root = std::shared_ptr(root_->Clone());
  std::shared_ptr<TrieNode> node = new_root;
  std::shared_ptr<TrieNode> parent = nullptr;
  std::stack<std::shared_ptr<TrieNode>> stack;

  for (char c : key) {
    if (node->children_.find(c) == node->children_.end()) {
      // Key not found, return original trie
      return *this;
    }
    parent = node;
    stack.push(parent);
    node = std::shared_ptr(node->children_.at(c)->Clone());
    parent->children_[c] = node;
  }

  // Now, node points to the key node and parent to its parent
  if (!node->is_value_node_) {
    // Key node not a value node, return original trie
    return *this;
  }
  uint64_t idx = key.size() - 1;
  char last = key[idx];
  if (!node->children_.empty()) {
    parent->children_[last] = std::make_shared<TrieNode>(node->children_);
    return Trie(new_root);
  }
  parent->children_.erase(key[idx]);
  while (!stack.empty() && !parent->is_value_node_ && parent->children_.empty()) {
    stack.pop();
    if (stack.empty()) {
      return Trie(nullptr);
    }
    parent = stack.top();
    idx--;
    parent->children_.erase(key[idx]);
  }
  return Trie(new_root);
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

#include "primer/trie.h"
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::GetValue(const std::shared_ptr<const TrieNode> &node) -> const T * {
  if (node != nullptr && node->is_value_node_) {
    auto ptr = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
    if (ptr == nullptr) {
      return nullptr;
    }
    return ptr->value_.get();
  }
  return nullptr;
}

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  //  throw NotImplementedException("Trie::Get is not implemented.");

  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
  if (key.empty()) {
    return GetValue<T>(root_);
  }

  uint32_t cur_pos = 0;
  auto cur_node = root_;

  while (cur_node != nullptr) {
    auto children_itr = cur_node->children_.find(key[cur_pos]);
    if (children_itr != cur_node->children_.end()) {
      ++cur_pos;
      cur_node = children_itr->second;
      if (cur_pos == key.size()) {
        return GetValue<T>(cur_node);
      }
    } else {
      return nullptr;
    }
  }
  return nullptr;
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  //  throw NotImplementedException("Trie::Put is not implemented.");

  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::shared_ptr<TrieNode> new_root;

  if (root_ != nullptr) {
    new_root = root_->Clone();
  } else {
    new_root = std::make_shared<TrieNode>();
  }

  std::shared_ptr<TrieNode> cur_node = new_root;
  std::shared_ptr<TrieNode> pre_node = nullptr;
  uint32_t cur_pos = 0;

  // walk through existing branch in the trie
  while (cur_pos < key.size()) {
    auto child_itr = cur_node->children_.find(key[cur_pos]);
    if (child_itr != cur_node->children_.end()) {
      std::shared_ptr<TrieNode> temp_node = child_itr->second->Clone();
      cur_node->children_[key[cur_pos]] = temp_node;
      pre_node = cur_node;
      cur_node = temp_node;
    } else {
      break;
    }
    ++cur_pos;
  }

  // walk through non-existing branch
  while (cur_pos < key.size()) {
    std::shared_ptr<TrieNode> temp_node = std::make_shared<TrieNode>();
    cur_node->children_[key[cur_pos]] = temp_node;
    pre_node = cur_node;
    cur_node = temp_node;
    ++cur_pos;
  }

  if (pre_node == nullptr) {
    new_root =
        std::shared_ptr<TrieNode>(new TrieNodeWithValue<T>(new_root->children_, std::make_shared<T>(std::move(value))));
  } else {
    pre_node->children_[key.back()] =
        std::shared_ptr<TrieNode>(new TrieNodeWithValue<T>(cur_node->children_, std::make_shared<T>(std::move(value))));
  }

  return Trie{std::move(new_root)};
}

auto Trie::Remove(std::string_view key) const -> Trie {
  //  throw NotImplementedException("Trie::Remove is not implemented.");

  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value anymore,
  // you should convert it to `TrieNode`. If a node doesn't have children anymore, you should remove it.

  if (root_ == nullptr) {
    return *this;
  }

  std::shared_ptr<TrieNode> new_root = root_->Clone();
  std::shared_ptr<TrieNode> cur_node = new_root;
  //  std::shared_ptr<TrieNode> pre_node = nullptr;
  std::stack<std::shared_ptr<TrieNode>> node_path;
  uint32_t cur_pos = 0;
  node_path.push(nullptr);

  // walk through existing branch in the trie
  while (cur_pos < key.size()) {
    auto child_itr = cur_node->children_.find(key[cur_pos]);
    if (child_itr != cur_node->children_.end()) {
      std::shared_ptr<TrieNode> temp_node = child_itr->second->Clone();
      cur_node->children_[key[cur_pos]] = temp_node;
      //      pre_node = cur_node;
      node_path.push(cur_node);
      cur_node = temp_node;
    } else {  // key doesn't exist
      return *this;
    }
    ++cur_pos;
  }

  if (!cur_node->is_value_node_) {
    return *this;
  }
  auto pre_node = node_path.top();
  if (!cur_node->children_.empty()) {
    if (pre_node == nullptr) {  // key is empty, cur_node is root
      new_root = std::make_shared<TrieNode>(new_root->children_);
    } else {
      pre_node->children_[key.back()] = std::make_shared<TrieNode>(cur_node->children_);
    }
    return Trie{std::move(new_root)};
  }

  // delete no child node along the path
  --cur_pos;
  while (!node_path.empty() && cur_node->children_.empty()) {
    pre_node = node_path.top();
    node_path.pop();
    if (pre_node != nullptr) {
      pre_node->children_.erase(key[cur_pos]);
      --cur_pos;
      cur_node = pre_node;
      if (cur_node->is_value_node_) {
        break;
      }
    } else {  // cur_node is root, and root has no child.
      new_root = nullptr;
    }
  }
  return Trie{std::move(new_root)};
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

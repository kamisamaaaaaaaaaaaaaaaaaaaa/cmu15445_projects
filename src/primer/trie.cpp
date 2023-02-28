#include "primer/trie.h"
#include <string_view>
#include "common/exception.h"

namespace bustub
{

  bool Trie::dfs_remove(std::shared_ptr<TrieNode> p, std::shared_ptr<TrieNode> fa, std::string_view &key, int u) const
  {
    if (u == key.size())
    {
      if (!p->is_value_node_)
        return false;

      if (p->children_.size() == 0)
      {
        p.reset();
        return true;
      }

      /*         p->is_value_node_ = false;*/
      fa->children_[key[u - 1]] = std::make_shared<TrieNode>(p->children_);
      p.reset();

      return false;
    }

    if (p->children_.count(key[u]) == 0)
      return false;

    if (dfs_remove(p->children_[key[u]], p, key, u + 1))
    {
      p->children_.erase(key[u]);
      if (p->children_.size() == 0 && !p->is_value_node_)
      {
        p.reset();
        return true;
      }
      return false;
    }

    return false;
  }

  std::shared_ptr<TrieNode> Trie::TrieClone(std::shared_ptr<TrieNode> u) const
  {
    auto t = u->Clone();
    for (auto &[k, v] : t->children_)
    {
      t->children_[k] = TrieClone(v);
    }
    return t;
  }

  template <class T>
  std::shared_ptr<TrieNodeWithValue<T>> Trie::TrieClone(std::shared_ptr<TrieNodeWithValue<T>> u) const
  {
    auto t = u->Clone();
    if (t->children_.size() == 0)
      return t;
    for (auto &[k, v] : t->children_)
    {
      t->children[k] = TrieClone(v);
    }
    return t;
  }

  template <class T>
  auto Trie::Get(std::string_view key) const -> T *
  {
    if (key.size() == 0)
      key = " ";
    auto p = root_;
    for (auto x : key)
    {
      if (p->children_.count(x) == 0)
        return nullptr;
      p = p->children_[x];
    }

    // if (!p->is_value_node_)
    //   return nullptr;

    std::shared_ptr<TrieNodeWithValue<T>> last = std::dynamic_pointer_cast<TrieNodeWithValue<T>>(p);

    if (last == nullptr)
      return nullptr;

    TrieNodeWithValue<T> *ptr = last.get();
    return ptr->value_.get();

    // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
    // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
    // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
    // Otherwise, return the value.
  }

  template <class T>
  auto Trie::Put(std::string_view key, T value) const -> Trie
  {
    // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
    auto n_trie = Trie(TrieClone(root_));
    auto p = n_trie.root_;

    if (key.size() == 0)
      key = " ";

    for (int i = 0; i < (int)key.size() - 1; i++)
    {
      char x = key[i];
      if (p->children_.count(x) == 0)
        p->children_[x] = std::make_shared<TrieNode>();
      p = p->children_[x];
    }

    if (p->children_.count(key.back()) == 0)
    {
      p->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(std::make_shared<T>(value));
    }
    else
    {
      p->children_[key.back()] = std::make_shared<TrieNodeWithValue<T>>(p->children_[key.back()]->children_, std::make_shared<T>(value));
    }

    return n_trie;

    // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
    // exists, you should create a new `TrieNodeWithValue`.
  }

  auto Trie::Remove(std::string_view key) const -> Trie
  {
    auto n_trie = Trie(TrieClone(root_));
    auto p = n_trie.root_;
    dfs_remove(p, nullptr, key, 0);

    return n_trie;

    // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
    // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
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

} // namespace bustub

#ifndef EARLIEST_ARRIVAL_H
#define EARLIEST_ARRIVAL_H
#include <grape/grape.h>

#include "temporal/earliest_arrival_context.h"

namespace grape {

template <typename FRAG_T>
class Temporal_EA : public AutoAppBase<FRAG_T, EAContext<FRAG_T>> {
 public:
  INSTALL_AUTO_WORKER(Temporal_EA<FRAG_T>, EAContext<FRAG_T>, FRAG_T);
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

 private:
  // 串行算法
  void ComputeEA(const fragment_t& frag, context_t& ctx,
                 std::priority_queue<std::pair<int64_t, vertex_t>>& que) {
    auto inner_vertices = frag.InnerVertices();

    VertexArray<typename FRAG_T::inner_vertices_t, bool> modified(
        inner_vertices, false);
    while (!que.empty()) {
      vertex_t u;
      int64_t arrival_time;
      u = que.top().second;
      arrival_time = -que.top().first;
      que.pop();
      ctx.partial_result.Reset(u);
      if (modified[u])
        continue;

      modified[u] = true;
      auto edges = frag.GetOutgoingAdjList(u);
      for (auto& edge : edges) {
        auto tmp = edge.get_data();
        if (tmp < arrival_time)
          continue;
        vertex_t v = edge.get_neighbor();
        int64_t distv = ctx.partial_result[v];
        // TODO: +1这里需要改成特殊的值
        int64_t ndistv = edge.get_data() + 1;
        if (distv > ndistv &&
            (ndistv <= ctx.end_time || ctx.end_time < ctx.start_time)) {
          ctx.partial_result.SetValue(v, ndistv);
          if (frag.IsInnerVertex(v)) {
            que.emplace(-ndistv, v);
          }
        }
      }
    }
  }

 public:
  /**
   * @brief
   */
  void PEval(const fragment_t& frag, context_t& ctx) {
    std::cout << "执行PEval" << std::endl;
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);
    std::priority_queue<std::pair<int64_t, vertex_t>> que;
    if (native_source) {
      ctx.partial_result.SetValue(source, ctx.start_time);
      que.emplace(-ctx.start_time, source);
    }
    ComputeEA(frag, ctx, que);
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    std::cout << "执行Inc" << std::endl;
    auto inner_vertices = frag.InnerVertices();
    std::priority_queue<std::pair<int64_t, vertex_t>> que;
    for (auto& v : inner_vertices) {
      if (ctx.partial_result.IsUpdated(v)) {
        que.emplace(-ctx.partial_result.GetValue(v), v);
      }
    }
    ComputeEA(frag, ctx, que);
  }
};
}  // namespace grape

#endif
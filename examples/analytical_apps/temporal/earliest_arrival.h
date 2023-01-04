#ifndef EARLIEST_ARRIVAL_H
#define EARLIEST_ARRIVAL_H
#include <grape/grape.h>

#include "temporal/earliest_arrival_context.h"

namespace grape {

template <typename FRAG_T>
class SSSPTeamporalEarliestArrival
    : public AutoAppBase<FRAG_T, EAContext<FRAG_T>> {
 public:
  INSTALL_AUTO_WORKER(SSSPTeamporalEarliestArrival<FRAG_T>, EAContext<FRAG_T>,
                      FRAG_T);
  using vertex_t = typename fragment_t::vertex_t;
  using vid_t = typename fragment_t::vid_t;

 private:
  // 串行算法
  void ComputeEA(const fragment_t& frag, context_t& ctx,
                 std::priority_queue<std::pair<long long, vertex_t>>& que) {
    auto inner_vertices = frag.InnerVertices();

    VertexArray<typename FRAG_T::inner_vertices_t, bool> modified(
        inner_vertices, false);
    while (!que.empty()) {
      vertex_t u;
      long long arrival_time;
      u = que.top().second;
      arrival_time = -que.top().first;
      ctx.partial_result.Reset(u);
      if (modified[u])
        continue;

      modified[u] = true;
      auto edges = frag.GetOutgoingAdjList(u);
      for (auto& edge : edges) {
        vertex_t v = edge.get_neighbor();
        long long distv = ctx.partial_result[v];
        // TODO: +1这里需要改成特殊的值
        long long ndistv = arrival_time + 1;
        if (distv > ndistv && (ndistv <= ctx.end_time || ctx.end_time == -1)) {
          ctx.partial_result.SetValue(v, ndistv);
          if (frag.IsInnerVertexv) {
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
    vertex_t source;
    bool native_source = frag.GetInnerVertex(ctx.source_id, source);
    std::priority_queue<std::pair<long long, vertex_t>> que;
    if (native_source) {
      ctx.partial_result.SetValue(source, ctx.start_time);
      que.emplace(ctx.start_time, source);
    }

    ComputeEA(frag, ctx, que);
  }

  void IncEval(const fragment_t& frag, context_t& ctx) {
    auto inner_vertices = frag.InnerVertices();
    std::priority_queue<std::pair<long long, vertex_t>> que;
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
use candle_core::{D, DType, Tensor};

/// Apply rotary position embeddings to `[batch, heads, tokens, head_dim]` attention states.
pub fn apply_rope(states: &Tensor, theta: f64) -> candle_core::Result<Tensor> {
    let (_, _, seq_len, head_dim) = states.dims4()?;
    let device = states.device();
    let dtype = states.dtype();
    let half_dim = head_dim / 2;
    let mut cos = Vec::with_capacity(seq_len * head_dim);
    let mut sin = Vec::with_capacity(seq_len * head_dim);
    for position in 0..seq_len {
        for dim in 0..head_dim {
            let freq_index = dim % half_dim;
            let inv_freq = theta.powf(-(2.0 * freq_index as f64) / head_dim as f64);
            let angle = position as f64 * inv_freq;
            cos.push(angle.cos() as f32);
            sin.push(angle.sin() as f32);
        }
    }

    let cos = Tensor::from_vec(cos, (1, 1, seq_len, head_dim), device)?.to_dtype(dtype)?;
    let sin = Tensor::from_vec(sin, (1, 1, seq_len, head_dim), device)?.to_dtype(dtype)?;
    let rotated = rotate_half(states)?;
    states
        .broadcast_mul(&cos)?
        .broadcast_add(&rotated.broadcast_mul(&sin)?)
}

/// Rotate the final dimension as `[-x2, x1]` for rotary embedding.
pub fn rotate_half(states: &Tensor) -> candle_core::Result<Tensor> {
    let (_, _, _, head_dim) = states.dims4()?;
    let half_dim = head_dim / 2;
    let first = states.narrow(3, 0, half_dim)?;
    let second = states.narrow(3, half_dim, half_dim)?.neg()?;

    Tensor::cat(&[&second, &first], 3)
}

/// Apply numerically stable softmax over the final dimension without fused kernels.
pub fn softmax_last_dim_metal_safe(scores: &Tensor) -> candle_core::Result<Tensor> {
    let output_dtype = scores.dtype();
    let scores = scores.to_dtype(DType::F32)?;
    let max = scores.max_keepdim(D::Minus1)?;
    let shifted = scores.broadcast_sub(&max)?;
    let exp = shifted.exp()?;
    let denominator = exp.sum_keepdim(D::Minus1)?;
    exp.broadcast_div(&denominator)?.to_dtype(output_dtype)
}

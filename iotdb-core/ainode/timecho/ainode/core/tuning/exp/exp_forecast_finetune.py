import os
import shutil
import time

import numpy as np
import torch
import torch.distributed as dist
import torch.nn.functional as F
from torch import optim
from transformers import get_scheduler

from timecho.ainode.core.tuning.exp.exp_basic import ExpBasic
from timecho.ainode.core.tuning.training_parameters import TuningParameters


class ExpForecastFinetune(ExpBasic):
    def __init__(self, rank: int, args: TuningParameters):
        super(ExpForecastFinetune, self).__init__(rank, args)

    def _select_optimizer(self):
        model_optim = optim.AdamW(
            self.model.parameters(),
            lr=self.args.learning_rate,
            weight_decay=self.args.weight_decay,
        )
        if self.rank == 0:
            self.logger.info("next learning rate is {}".format(self.args.learning_rate))
        return model_optim

    def finetune(self, log_chunk=100):
        time_start = time.time()
        model_optim = self._select_optimizer()
        scheduler = get_scheduler(
            "cosine",
            optimizer=model_optim,
            num_warmup_steps=self.args.num_warmup_steps,
            num_training_steps=self.args.num_training_steps,
        )
        for epoch in range(0, self.args.train_epochs):
            if self.args.ddp:
                self.training_dataloader.sampler.set_epoch(epoch)
            iter_count = 0
            self.model.train()
            epoch_start_time = time.time()
            iter_log_start_time = time.time()
            for i, (batch_x, batch_y, loss_mask) in enumerate(self.training_dataloader):
                iter_count += 1
                model_optim.zero_grad()
                batch_x = batch_x.float().to(self.gpu_id)
                batch_y = batch_y.float().to(self.gpu_id)
                loss_mask = loss_mask.float().to(self.gpu_id)

                outputs = self.model(
                    input_ids=batch_x,
                    labels=batch_y,
                    loss_masks=loss_mask,
                    revin=self.args.revin,
                )
                loss = outputs["loss"] if isinstance(outputs, dict) else outputs[0]
                # criterion = torch.nn.MSELoss()
                # loss = criterion(outputs, batch_y)
                # logger.info('\nmodel loss:\n', loss)

                loss.backward()
                model_optim.step()
                scheduler.step()

                if (i + 1) % log_chunk == 0 or (i + 1) == len(self.training_dataloader):
                    speed = (time.time() - iter_log_start_time) / iter_count
                    left_time = speed * (len(self.training_dataloader) - i)
                    self.logger.info(
                        "[Training][GPU-{}] Iters: {}, speed: {:.4f}s/iter, left time: {:.4f}s".format(
                            self.gpu_id, i + 1, speed, left_time
                        )
                    )
                    iter_count = 0
                    iter_log_start_time = time.time()

            epoch_mse, epoch_mae = self.validation()
            if self.rank == 0:
                self.mse_loss_list.append(epoch_mse)
                self.mae_loss_list.append(epoch_mae)
                self.logger.info(
                    "[Training][GPU-{}] Epoch: {} cost time: {}, speed: {:.4f}s/epoch, lr = {:.10f}".format(
                        self.gpu_id,
                        epoch,
                        time.time() - epoch_start_time,
                        (time.time() - time_start) / (epoch + 1),
                        model_optim.param_groups[0]["lr"],
                    )
                )
                self.logger.info(
                    "[Training][GPU-{}] Epoch: {}, current loss: {:.7f}, mse_list: {}".format(
                        self.gpu_id,
                        epoch,
                        loss.item(),
                        self.mse_loss_list,
                        # self.mae_loss_list,
                    )
                )
                # save the current model ckpt
                torch.cuda.empty_cache()
                # TODO: The ModelManager should take over this process
                save_dir = os.path.join(
                    self._model_dir, self.args.model_id + "_" + str(epoch)
                )
                os.makedirs(save_dir, exist_ok=True)
                self.model.module.save_pretrained(save_dir)
                self.logger.info(
                    "[Training] Model: {} saved!".format(
                        self.args.model_id + "_" + str(epoch)
                    )
                )

            dist.barrier()
        if self.rank == 0:
            # Save the best model, TODO: The ModelManager should take over this process
            best_index = np.argmin(np.array(self.mse_loss_list))
            final_dir = os.path.join(self._fine_tuned_model_dir, self.args.model_id)
            for i in range(0, self.args.train_epochs):
                save_dir = os.path.join(
                    self._model_dir, self.args.model_id + "_" + str(i)
                )
                if i != best_index and self.args.only_preserve_best:
                    # delete sub-optimal model checkpoints
                    if os.path.exists(save_dir):
                        shutil.rmtree(save_dir)
                else:
                    # move the best model checkpoint
                    if os.path.exists(save_dir):
                        if os.path.exists(final_dir):
                            shutil.rmtree(final_dir)
                        os.makedirs(final_dir, exist_ok=True)
                        for file in os.listdir(save_dir):
                            src_file = os.path.join(save_dir, file)
                            dst_file = os.path.join(final_dir, file)
                            if os.path.exists(dst_file):
                                os.remove(dst_file)
                            shutil.move(src_file, dst_file)
                        shutil.rmtree(save_dir)
            self.logger.info(
                "[Training] Model: {} preserves epoch {}".format(
                    self.args.model_id, best_index
                )
            )

    # * Model Evaluation Function
    def validation(self, log_chunk=100):
        total_mse_loss = torch.tensor(0.0).to(self.gpu_id)
        total_mae_loss = torch.tensor(0.0).to(self.gpu_id)
        total_count = torch.tensor(0.0).to(self.gpu_id)
        iter_count = 0
        time_now = time.time()
        test_steps = len(self.vali_dataloader)
        self.model.eval()
        with torch.no_grad():
            chunk_mse_loss = torch.tensor(0.0).to(self.gpu_id)
            chunk_mae_loss = torch.tensor(0.0).to(self.gpu_id)
            chunk_count = torch.tensor(0.0).to(self.gpu_id)
            for i, (batch_x, batch_y, loss_mask) in enumerate(self.vali_dataloader):
                iter_count += 1
                batch_x = batch_x.float().to(self.gpu_id)
                batch_y = batch_y.float().to(self.gpu_id)

                B = batch_x.shape[0]
                if "timer" == self.args.model_type:
                    pred = self.model.module.generate(
                        batch_x,
                        max_new_tokens=self.args.vali_pred_len,
                        revin=self.args.revin,
                    )
                elif "sundial" == self.args.model_type:
                    outputs = self.model.module.generate(
                        batch_x,
                        max_new_tokens=self.args.vali_pred_len,
                        num_samples=self.args.vali_n_samples,
                        revin=self.args.revin,
                    )
                    outputs = outputs.reshape(self.args.vali_n_samples, B, -1)
                    pred = outputs.mean(dim=0)
                if self.args.vali_pred_len < self.args.output_token_len:
                    batch_y = batch_y[
                        :,
                        -self.args.output_token_len : -self.args.output_token_len
                        + self.args.vali_pred_len,
                    ]
                else:
                    batch_y = batch_y[:, -self.args.output_token_len :]

                mse_loss = (
                    F.mse_loss(pred, batch_y, reduction="none").mean(dim=-1).sum()
                )
                mae_loss = F.l1_loss(pred, batch_y, reduction="none").mean(dim=-1).sum()

                chunk_mse_loss += mse_loss
                chunk_mae_loss += mae_loss
                chunk_count += B

                if (i + 1) % log_chunk == 0 or (i + 1) == len(self.vali_dataloader):
                    dist.barrier()
                    dist.reduce(chunk_mse_loss, dst=0, op=dist.ReduceOp.SUM)
                    dist.reduce(chunk_mae_loss, dst=0, op=dist.ReduceOp.SUM)
                    dist.reduce(chunk_count, dst=0, op=dist.ReduceOp.SUM)

                    # Accumulate global loss
                    total_mse_loss += chunk_mse_loss
                    total_mae_loss += chunk_mae_loss
                    total_count += chunk_count

                    # Reset chunk metrics
                    chunk_mse_loss.zero_()
                    chunk_mae_loss.zero_()
                    chunk_count.zero_()

                    speed = (time.time() - time_now) / iter_count
                    left_time = speed * (test_steps - i)
                    self.logger.info(
                        "[Validation][GPU-{}] Iters: {}, speed: {:.4f}s/iter, left time: {:.4f}s".format(
                            self.gpu_id, i + 1, speed, left_time
                        )
                    )
                    iter_count = 0
                    time_now = time.time()
        self.model.train()
        total_mse_loss = total_mse_loss.item() / total_count.item()
        total_mae_loss = total_mae_loss.item() / total_count.item()
        return total_mse_loss, total_mae_loss

import os
import time

import torch
import torch.distributed as dist
import torch.nn.functional as F
from torch import optim
from torchmetrics.regression import MeanAbsolutePercentageError
from transformers import get_scheduler

from ainode.core.log import Logger
from ainode.core.training.exp.exp_basic import ExpBasic
from ainode.core.training.training_parameters import TrainingParameters

logger = Logger()


class ExpForecastFinetune(ExpBasic):
    def __init__(self, rank: int, args: TrainingParameters):
        super(ExpForecastFinetune, self).__init__(rank, args)

    def _select_optimizer(self):
        model_optim = optim.AdamW(
            self.model.parameters(),
            lr=self.args.learning_rate,
            betas=(0.9, 0.95),
            weight_decay=self.args.weight_decay,
        )
        if self.rank == 0:
            logger.info("next learning rate is {}".format(self.args.learning_rate))
        return model_optim

    def train(self):
        time_now = time.time()
        # Load model weights through specific adaptation method
        if self.args.adaptation == "linear":
            # * linear probing adaptation
            # if 'sundial' in self.args.model_type:
            # * new config init
            config = self.config_dict[self.args.model_type].SundialConfig(
                input_token_len=self.args.input_token_len,
                output_token_lens=[self.args.max_output_token_len],
            )

            # * HF from_pretrained loading ignoring mismatched sizes
            # * - Only loading matched weights
            # * - Unmatched weights remain pre-defined
            self.model.module = self.model.module.from_pretrained(
                self.args.ckpt_path,
                config=config,
                # device_map=self.device,
                ignore_mismatched_sizes=True,
                torch_dtype=torch.float32,
            ).to(self.gpu_id)

            # * Linear Probing: freezing everything except linear head
            for name, param in self.model.module.named_parameters():
                if "flow_loss" in name:
                    # param.data.zero_() # * Zero Initialization for linear probing params
                    param.requires_grad = True
                else:
                    param.requires_grad = False
                if self.rank == 0:
                    pass
                    # logger.info(f"{name}: {param.requires_grad}")

            model_optim = self._select_optimizer()
            scheduler = get_scheduler(
                "constant",
                optimizer=model_optim,
                num_warmup_steps=self.args.num_warmup_steps,
                num_training_steps=self.args.num_training_steps,
            )
        else:
            raise NotImplementedError(
                "[Training][GPU-{}]Adaptation method {} is not implemented.".format(
                    self.gpu_id, self.args.adaptation
                )
            )

        for epoch in range(0, self.args.train_epochs):
            # TODO: Is it necessary to set epoch?
            # if self.args.ddp:
            #     self.training_dataloader.sampler.set_epoch(epoch)
            iter_count = 0
            self.model.train()
            epoch_time = time.time()
            for i, (batch_x, batch_y, loss_mask) in enumerate(self.training_dataloader):
                if i >= self.args.iter_per_epoch:
                    break
                iter_count += 1
                model_optim.zero_grad()
                batch_x = batch_x.float().to(self.gpu_id)
                batch_y = batch_y.float().to(self.gpu_id)
                loss_mask = loss_mask.float().to(self.gpu_id)

                outputs = self.model(
                    input_ids=batch_x,
                    labels=batch_y,
                    loss_masks=loss_mask,
                    revin=self.args.test_with_revin,
                )
                loss = outputs["loss"] if isinstance(outputs, dict) else outputs[0]
                # criterion = torch.nn.MSELoss()
                # loss = criterion(outputs, batch_y)

                # logger.info('\nmodel loss:\n', loss)
                if (i + 1) % 1000 == 0:
                    if self.rank == 0:
                        logger.info(
                            "\t[Training][GPU-{}] Iters: {}, epoch: {} | loss: {:.7f}".format(
                                self.gpu_id, i + 1, epoch + 1, loss.item()
                            )
                        )
                        speed = (time.time() - time_now) / iter_count
                        logger.info(
                            "\t[Training][GPU-{}] Speed: {:.4f}s/iter, lr = {:.10f}".format(
                                self.gpu_id, speed, model_optim.param_groups[0]["lr"]
                            )
                        )
                        iter_count = 0
                        time_now = time.time()

                loss.backward()
                model_optim.step()
                scheduler.step()

            if self.rank == 0:
                logger.info(
                    "[Training] Epoch: {} cost time: {}".format(
                        epoch + 1, time.time() - epoch_time
                    )
                )

            # * TEST IN TRAINING
            # TODO: How to compare the best ckpt and stop even earlier?
            # self.vali()
            # torch.cuda.empty_cache()
            # save_dir = os.path.join(self._model_dir, self.args.model_id + f"checkpoint_{epoch}")
            # if dist.get_rank() == 0:
            #     os.makedirs(save_dir, exist_ok=True)
            #     self.model.module.save_pretrained(save_dir)
            #     logger.info("Model: {} epoch: {} saved!".format(self.args.model_id, epoch))
            dist.barrier()
        if self.rank == 0:
            # Save the final model, TODO: The ModelManager should take over this process
            save_dir = os.path.join(self._model_dir, "weights", self.args.model_id)
            os.makedirs(save_dir, exist_ok=True)
            self.model.module.save_pretrained(save_dir)
            logger.info("[Training] Model: {} saved!".format(self.args.model_id))

    def vali(self, vali_n_sample=1, chunk_size=100):
        total_mse_loss = torch.tensor(0.0).to(self.gpu_id)
        total_mae_loss = torch.tensor(0.0).to(self.gpu_id)
        total_inverse_mse_loss = torch.tensor(0.0).to(self.gpu_id)
        total_inverse_mae_loss = torch.tensor(0.0).to(self.gpu_id)
        total_inverse_mape_loss = torch.tensor(0.0).to(self.gpu_id)
        total_count = torch.tensor(0.0).to(self.gpu_id)
        iter_count = 0
        time_now = time.time()
        test_steps = len(self.valid_dataloader)
        self.model.eval()
        with torch.no_grad():
            chunk_mse_loss = torch.tensor(0.0).to(self.gpu_id)
            chunk_mae_loss = torch.tensor(0.0).to(self.gpu_id)
            inverse_chunk_mse_loss = torch.tensor(0.0).to(self.gpu_id)
            inverse_chunk_mae_loss = torch.tensor(0.0).to(self.gpu_id)
            inverse_chunk_mape_loss = torch.tensor(0.0).to(self.gpu_id)
            chunk_count = torch.tensor(0.0).to(self.gpu_id)

            for i, (batch_x, batch_y, loss_mask) in enumerate(self.valid_dataloader):
                iter_count += 1
                batch_x = batch_x.float().to(self.gpu_id)
                batch_y = batch_y.float().to(self.gpu_id)
                # input_mask = input_mask.float().to(self.device)

                B = batch_x.shape[0]
                batch_x = batch_x.repeat(vali_n_sample, 1)
                if self.args.ddp:
                    outputs = self.model.module.generate(
                        batch_x,
                        max_new_tokens=self.args.test_pred_len,
                        revin=self.args.test_with_revin,
                    )
                else:
                    outputs = self.model.generate(
                        batch_x,
                        max_new_tokens=self.args.test_pred_len,
                        revin=self.args.test_with_revin,
                    )
                outputs = outputs.reshape(vali_n_sample, B, -1)
                pred = outputs.mean(dim=0)
                batch_y = batch_y[:, -self.args.test_pred_len :]

                mse_loss = (
                    F.mse_loss(pred, batch_y, reduction="none").mean(dim=-1).sum()
                )
                mae_loss = F.l1_loss(pred, batch_y, reduction="none").mean(dim=-1).sum()

                inverse_pred = self.valid_dataset.inverse_transform(pred)
                inverse_batch_y = self.valid_dataset.inverse_transform(batch_y)
                inverse_mse_loss = (
                    F.mse_loss(inverse_pred, inverse_batch_y, reduction="none")
                    .mean(dim=-1)
                    .sum()
                )
                inverse_mae_loss = (
                    F.l1_loss(inverse_pred, inverse_batch_y, reduction="none")
                    .mean(dim=-1)
                    .sum()
                )
                mape_loss = MeanAbsolutePercentageError().to(pred.device)
                inverse_mape_loss = mape_loss(inverse_pred, inverse_batch_y)

                chunk_mse_loss += mse_loss
                chunk_mae_loss += mae_loss
                inverse_chunk_mse_loss += inverse_mse_loss
                inverse_chunk_mae_loss += inverse_mae_loss
                inverse_chunk_mape_loss += inverse_mape_loss
                chunk_count += B

                if (i + 1) % chunk_size == 0 or (i + 1) == len(self.valid_dataloader):
                    if self.args.ddp:
                        dist.barrier()
                        dist.reduce(chunk_mse_loss, dst=0, op=dist.ReduceOp.SUM)
                        dist.reduce(chunk_mae_loss, dst=0, op=dist.ReduceOp.SUM)
                        dist.reduce(inverse_chunk_mse_loss, dst=0, op=dist.ReduceOp.SUM)
                        dist.reduce(inverse_chunk_mae_loss, dst=0, op=dist.ReduceOp.SUM)
                        dist.reduce(
                            inverse_chunk_mape_loss, dst=0, op=dist.ReduceOp.SUM
                        )
                        dist.reduce(chunk_count, dst=0, op=dist.ReduceOp.SUM)

                    # Accumulate global loss
                    total_mse_loss += chunk_mse_loss
                    total_mae_loss += chunk_mae_loss
                    total_inverse_mse_loss += inverse_chunk_mse_loss
                    total_inverse_mae_loss += inverse_chunk_mae_loss
                    total_inverse_mape_loss += inverse_chunk_mape_loss
                    total_count += chunk_count

                    # Reset chunk metrics
                    chunk_mse_loss.zero_()
                    chunk_mae_loss.zero_()
                    inverse_chunk_mse_loss.zero_()
                    inverse_chunk_mae_loss.zero_()
                    inverse_chunk_mse_loss.zero_()
                    chunk_count.zero_()

                    if self.rank == 0:
                        speed = (time.time() - time_now) / iter_count
                        left_time = speed * (test_steps - i)
                        logger.info(
                            "\t[Training][GPU-{}] Iters: {}, speed: {:.4f}s/iter, left time: {:.4f}s".format(
                                self.gpu_id, i + 1, speed, left_time
                            )
                        )
                        iter_count = 0
                        time_now = time.time()
        total_mse_loss = total_mse_loss.item() / total_count.item()
        total_mae_loss = total_mae_loss.item() / total_count.item()
        total_inverse_mse_loss = total_inverse_mse_loss.item() / total_count.item()
        total_inverse_mae_loss = total_inverse_mae_loss.item() / total_count.item()
        total_inverse_mape_loss = total_inverse_mape_loss.item() / total_count.item()
        self.model.train()
        return (
            total_mse_loss,
            total_mae_loss,
            total_inverse_mse_loss,
            total_inverse_mae_loss,
            total_inverse_mape_loss,
        )

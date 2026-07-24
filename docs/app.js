const toast = document.querySelector(".toast");
let toastTimer;

async function copyText(text, button) {
  await navigator.clipboard.writeText(text);
  const original = button.textContent;
  if (button.matches(".window-bar button")) button.textContent = "COPIED";
  toast.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => {
    toast.classList.remove("show");
    if (button.matches(".window-bar button")) button.textContent = original;
  }, 1700);
}

document.querySelectorAll("[data-copy]").forEach((button) => {
  button.addEventListener("click", () => copyText(button.dataset.copy, button));
});

document.querySelectorAll("[data-copy-target]").forEach((button) => {
  button.addEventListener("click", () => {
    const target = document.getElementById(button.dataset.copyTarget);
    copyText(target.textContent, button);
  });
});

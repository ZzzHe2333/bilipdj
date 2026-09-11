(() => {
  const year = document.querySelector('#year');
  if (year) year.textContent = new Date().getFullYear();

  const setText = (selector, value) => {
    const el = document.querySelector(selector);
    if (el && value) el.textContent = value;
  };

  fetch('https://api.github.com/repos/ZzzHe2333/bilipdj/releases/latest', {
    headers: { Accept: 'application/vnd.github+json' }
  })
    .then((response) => response.ok ? response.json() : null)
    .then((data) => {
      if (data?.tag_name) setText('#releaseValue', data.tag_name);
    })
    .catch(() => {});

  fetch('https://api.github.com/repos/ZzzHe2333/bilipdj', {
    headers: { Accept: 'application/vnd.github+json' }
  })
    .then((response) => response.ok ? response.json() : null)
    .then((data) => {
      if (typeof data?.stargazers_count === 'number') {
        setText('#starsValue', `${data.stargazers_count} Stars`);
      }
    })
    .catch(() => {});
})();

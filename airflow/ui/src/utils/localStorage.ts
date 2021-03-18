export function set(key: string, value:string): Record<string, unknown> {
  localStorage[key] = value;

  // Set session expiry (24hrs)
  if (key === 'token') {
    const date = new Date();
    localStorage[`${key}-expire`] = new Date(date.getTime() + 86400000);
  }

  return localStorage[key];
}

export function get(key: string, defaultValue = undefined): string {
  const value = localStorage[key] || defaultValue;
  return value;
}

export function clear(): void {
  return localStorage.clear();
}

export function remove(key: string): void {
  return localStorage.removeItem(key);
}

export function checkExpire(key: string): boolean {
  const sessExpire = get(`${key}-expire`);
  const sess = get(key);
  if (!sessExpire || !sess) return true;
  return new Date() > new Date(sessExpire);
}

export function clearAuth(): void {
  localStorage.removeItem('token');
  localStorage.removeItem('token-expire');
}

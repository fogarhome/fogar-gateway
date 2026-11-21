// El nombre con el que este navegador aparece en «Mis dispositivos»: navegador y sistema, sin más
// detalle (no hace falta saber la versión para reconocerlo).
export function deviceName(ua: string): string {
  const navegador = /Edg\//.test(ua)
    ? 'Edge'
    : /Firefox\//.test(ua)
      ? 'Firefox'
      : /Chrome\//.test(ua)
        ? 'Chrome'
        : /Safari\//.test(ua)
          ? 'Safari'
          : 'Navegador'
  const sistema = /iPhone/.test(ua)
    ? 'iPhone'
    : /iPad/.test(ua)
      ? 'iPad'
      : /Android/.test(ua)
        ? 'Android'
        : /Mac OS X|Macintosh/.test(ua)
          ? 'Mac'
          : /Windows/.test(ua)
            ? 'Windows'
            : /Linux/.test(ua)
              ? 'Linux'
              : null
  return sistema ? `${navegador} en ${sistema}` : navegador
}

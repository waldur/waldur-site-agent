FROM rockylinux:9
RUN dnf -y install --setopt=install_weak_deps=False \
        sssd sssd-ldap sssd-tools openldap-clients shadow-utils util-linux \
    && dnf clean all

COPY sssd.conf /etc/sssd/sssd.conf
RUN chmod 600 /etc/sssd/sssd.conf && chown root:root /etc/sssd/sssd.conf

# Wire NSS and PAM to SSSD by hand rather than via authselect: it is two
# explicit files, and it keeps visible exactly what makes the directory
# resolve, which is the point of this demo.
RUN printf '%s\n' \
      'passwd:     files sss' \
      'shadow:     files sss' \
      'group:      files sss' \
      'hosts:      files dns' \
      'services:   files sss' \
      'netgroup:   files sss' \
      'automount:  files sss' \
    > /etc/nsswitch.conf
RUN printf '%s\n' \
      'auth        required      pam_env.so' \
      'auth        sufficient    pam_unix.so nullok try_first_pass' \
      'auth        sufficient    pam_sss.so forward_pass' \
      'auth        required      pam_deny.so' \
      'account     sufficient    pam_unix.so' \
      'account     sufficient    pam_sss.so' \
      'account     required      pam_deny.so' \
      'password    sufficient    pam_unix.so try_first_pass use_authtok nullok sha512 shadow' \
      'password    sufficient    pam_sss.so use_authtok' \
      'password    required      pam_deny.so' \
      'session     optional      pam_keyinit.so revoke' \
      'session     required      pam_limits.so' \
      'session     required      pam_unix.so' \
      'session     optional      pam_sss.so' \
      'session     optional      pam_mkhomedir.so skel=/etc/skel umask=0077' \
    > /etc/pam.d/system-auth \
    && cp /etc/pam.d/system-auth /etc/pam.d/password-auth

CMD ["/usr/sbin/sssd", "-i", "--logger=stderr"]
